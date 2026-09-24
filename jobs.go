package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

// startJobs 进程内定时任务：每分钟一轮，小时/天级任务按上次执行时间判断。
func (a *App) startJobs() {
	a.startTG()
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		a.repairCertPayments()
		a.runJobs()
		for {
			select {
			case <-a.stop:
				return
			case <-t.C:
				a.runJobs()
			}
		}
	}()
}

func (a *App) runJobs() {
	if !a.jobMu.TryLock() {
		return
	}
	defer a.jobMu.Unlock()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[error] 定时任务 panic: %v", r)
		}
	}()
	now := ms()
	a.expireClaims(now)
	a.retryVerifies(now)
	a.runRechecks(now)
	a.overduePayables(now)
	a.autoDefaultOverdue(now)
	a.autoApproveCheckings(now)
	a.autoConfirmAwaiting(now)
	a.closeTasks()
	a.reviewTick(now)
	a.syncPending()
	a.routeDisputes(now)
	a.juryTick()
	last, _ := a.st.GetMeta("last_hourly")
	if lastMs := atoi64(last); now-lastMs >= hourMs {
		a.st.SetMeta("last_hourly", fmt.Sprint(now))
		a.ipTouched.Range(func(k, v any) bool { // 节流表只需要最近 10 分钟，超过 1 小时的键清掉，别跟着进程一直长
			if t, ok := v.(int64); ok && now-t > hourMs {
				a.ipTouched.Delete(k)
			}
			return true
		})
		a.checkGatewayHealth()
		since := lastMs
		if since == 0 || now-since > 7*dayMs {
			since = now - hourMs
		}
		a.remind(now, since)
		a.syncExpired()
	}
	lastD, _ := a.st.GetMeta("last_daily")
	if lastMs := atoi64(lastD); now-lastMs >= dayMs {
		a.st.SetMeta("last_daily", fmt.Sprint(now))
		a.st.PruneVerify()
		a.st.db.Exec(`DELETE FROM tweet_cache WHERE fetched_at<? AND tweet_id NOT IN (SELECT tweet_id FROM submissions WHERE tweet_id<>'') AND tweet_id NOT IN (SELECT reg_tweet_id FROM users WHERE reg_tweet_id<>'')`, now-180*dayMs)
		a.st.db.Exec(`DELETE FROM notifications WHERE created_at<?`, now-90*dayMs)
		a.st.db.Exec(`DELETE FROM ip_log WHERE last_seen<?`, now-90*dayMs)
		a.st.db.Exec(`DELETE FROM ip_seen WHERE last_seen<?`, now-90*dayMs)
		a.st.db.Exec(`DELETE FROM device_seen WHERE last_seen<?`, now-90*dayMs)
		a.backup()
	}
}

func atoi64(s string) int64 {
	var n int64
	fmt.Sscan(s, &n)
	return n
}

func (a *App) expireClaims(now int64) {
	subs, _ := a.st.ExpiredClaims(now)
	for _, x := range subs {
		if ok, _ := a.st.SetExpired(x.ID); ok {
			a.st.Audit(0, "sub.expired", "submission", x.ID, nil, "")
			a.notify(x.WorkerID, "verify", "接单已过期", "记录 "+x.Code+" 未在时限内提交，名额已释放。", x.Path())
			if t, _ := a.st.GetTaskByID(x.TaskID); t != nil {
				if w, _ := a.st.GetUserByID(x.WorkerID); w != nil {
					a.notify(t.OwnerID, "task", "@"+w.Handle+" 接单超时，名额已释放", "《"+t.Title+"》可接名额 +1。", t.Path()+"#manage")
				}
			}
		}
	}
}

func (a *App) retryVerifies(now int64) {
	subs, _ := a.st.VerifyRetries(now)
	for _, x := range subs {
		a.verifySubmission(x)
	}
}

func (a *App) runRechecks(now int64) {
	subs, _ := a.st.DueRechecks(now)
	for _, x := range subs {
		a.recheckSubmission(x, false)
	}
}

func (a *App) overduePayables(now int64) {
	subs, _ := a.st.DuePayables(now)
	for _, x := range subs {
		if ok, _ := a.st.SetOverdue(x.ID); !ok {
			continue
		}
		a.st.Audit(0, "sub.overdue", "submission", x.ID, nil, "")
		t, _ := a.st.GetTaskByID(x.TaskID)
		if t == nil {
			continue
		}
		n := a.st.FreezeOwnerTasks(t.OwnerID)
		auto := ""
		if a.cfg.OverdueAutoDefaultH > 0 {
			auto = fmt.Sprintf("逾期满 %s 仍未付清（或未登记已付），系统会自动记为违约并将你列入黑名单，不需要对方举报。", dur(a.cfg.OverdueAutoDefaultH))
		}
		a.notify(t.OwnerID, "pay", "记录 "+x.Code+" 已逾期未付", fmt.Sprintf("你的账号已冻结（%d 个任务暂停接单），付清后自动恢复。接单方可以举报，核实后将进入黑名单。%s", n, auto), x.Path())
		wauto := ""
		if a.cfg.OverdueAutoDefaultH > 0 {
			wauto = fmt.Sprintf("逾期满 %s 系统会自动记为违约并将对方列入黑名单，不举报也会处理；举报可以加快核实。", dur(a.cfg.OverdueAutoDefaultH))
		}
		a.notify(x.WorkerID, "pay", "发布方逾期未付款", "记录 "+x.Code+" 已逾期，你可以在记录页举报；对方付清会自动通知你。"+wauto, x.Path())
	}
}

// autoDefaultOverdue 逾期满 OverdueAutoDefaultH 仍未付：不等接单方举报，自动记为违约并把发布方列入黑名单。
// 两步走：先在到点前 24 小时（至少）警告发布方一次（登记已付 / 付清都能解除），警告满 24 小时且逾期满阈值才动手。
// 上黑名单的效果与 A 类举报成立完全一致（名下逾期记录全部转违约、欠款公示、任务关闭）；误伤可走 F 类申诉。
func (a *App) autoDefaultOverdue(now int64) {
	h := a.cfg.OverdueAutoDefaultH
	if h <= 0 {
		return
	}
	// 1) 警告：逾期时长达到 (阈值 - 24h) 的记录，按发布方合并成一条通知
	warnLine := now - max(h-24, 0)*hourMs
	if subs, _ := a.st.OverdueToWarn(warnLine); len(subs) > 0 {
		byOwner := map[int64][]*Submission{}
		for _, x := range subs {
			if t, _ := a.st.GetTaskByID(x.TaskID); t != nil {
				byOwner[t.OwnerID] = append(byOwner[t.OwnerID], x)
			}
		}
		for owner := range byOwner {
			// 一个发布方只警告一次：把名下所有逾期（含举报中）记录一起标记、合计欠款
			all, _ := a.st.SubsByOwnerStatus(owner, SOverdue, SDisputed)
			var ids []int64
			var sum int64
			for _, x := range all {
				if x.Status == SDisputed && x.PrevStatus != SOverdue {
					continue
				}
				ids = append(ids, x.ID)
				if t, _ := a.st.GetTaskByID(x.TaskID); t != nil {
					sum += max(payAmount(x, t)-x.UnderpaidE8, 0)
				}
				if x.DefaultWarnedAt == 0 {
					a.st.Audit(0, "sub.default_warn", "submission", x.ID, map[string]any{"owed": fmtE8(sum)}, "")
				}
			}
			a.st.SetDefaultWarned(ids)
			if bl, _ := a.st.ActiveBlacklist(owner); bl != nil {
				continue // 已在榜的不用再吓唬
			}
			a.notify(owner, "pay", fmt.Sprintf("最后提醒：%d 笔逾期未付将自动记为违约", len(ids)), fmt.Sprintf("共 %s U。24 小时内付清（自动到账）或在记录页登记已付，否则系统会自动记为违约并将你列入黑名单，名下任务全部关闭。", fmtE8(sum)), "/me")
		}
	}
	// 2) 违约上榜：逾期满阈值，且警告已满 24 小时
	subs, _ := a.st.OverdueToDefault(now-h*hourMs, now-dayMs)
	done := map[int64]bool{}
	for _, x := range subs {
		t, _ := a.st.GetTaskByID(x.TaskID)
		if t == nil || done[t.OwnerID] {
			continue
		}
		done[t.OwnerID] = true
		owner, _ := a.st.GetUserByID(t.OwnerID)
		if owner == nil {
			continue
		}
		a.st.Audit(0, "blacklist.auto", "user", owner.ID, map[string]any{"record": x.Code, "overdue_h": (now - x.OverdueAt) / hourMs}, "")
		a.blacklistUser(owner, "publisher", fmt.Sprintf("逾期 %s 仍未付款，自动记为违约", dur(h)), 0, "")
		// 该发布方名下正在举报流程里、还没交小法庭的 A 类申诉：结论已定，直接成立结案
		if ds, _ := a.st.queryDisputes(`WHERE type='A' AND status<>'resolved' AND jury_case_id=0 AND submission_id IN (SELECT id FROM submissions WHERE status='defaulted' AND task_id IN (SELECT id FROM tasks WHERE owner_id=?))`, owner.ID); len(ds) > 0 {
			for _, d := range ds {
				if !a.st.ResolveDisputeIfOpen(d.ID, "upheld", "发布方逾期超时，系统已自动记为违约并列入黑名单，举报成立") {
					continue
				}
				a.st.Audit(0, "dispute.auto_close", "dispute", d.ID, map[string]any{"by": "auto_default"}, "")
				a.notify(d.OpenerID, "dispute", "举报 "+d.Code+" 已成立", "发布方逾期超时，系统已自动将其列入黑名单，记录转为违约；对方补付后会通知你。", d.Path())
			}
		}
	}
}

func (a *App) closeTasks() {
	ts, _ := a.st.TasksToClose()
	for _, t := range ts {
		a.st.SetTaskStatus(t.ID, "closed", "deadline", false)
	}
}

// syncPending 每分钟主动对账 pending 的网关订单（回调丢了也能补）。
func (a *App) syncPending() {
	if a.gwc == nil {
		return
	}
	ps, _ := a.st.PendingPayments()
	for _, p := range ps {
		if p.ExpiresAt > 0 && p.ExpiresAt+35*60*1000 < ms() {
			// 早该过期却没收到回调：查一次，网关说过期就落过期
			a.syncPaymentNow(p)
			continue
		}
		if a.st.TouchSync(p.ID, ms(), 50*1000) {
			a.syncPaymentNow(p)
		}
	}
}

// syncExpired 每小时对 7 天内过期的订单再查一次（回填窗口内可能变 paid）。
func (a *App) syncExpired() {
	if a.gwc == nil {
		return
	}
	ps, _ := a.st.ExpiredRecentPayments(50 * 60 * 1000)
	for _, p := range ps {
		a.st.TouchSync(p.ID, ms(), 0)
		a.syncPaymentNow(p)
	}
}

// routeDisputes 举证期满 → 小法庭或管理员；上诉期满 → 执行裁决。
func (a *App) routeDisputes(now int64) {
	ds, _ := a.st.EvidenceExpired(now)
	for _, d := range ds {
		if d.Type == "A" {
			// A 类：宽限期内已付则 afterPaid 已结案；这里只把未付的送去核实
			if x, _ := a.st.GetSubByID(d.SubmissionID); x != nil && x.Status != SDisputed {
				if x.Status == SPaid || x.Status == SAwait {
					a.st.ResolveDispute(d.ID, "resolved_by_payment", "款项已到账，自动结案", 0)
				} else {
					a.st.ResolveDispute(d.ID, "resolved_by_state", "记录已转为"+subStatus(x.Status)+"，申诉随之结案", 0)
				}
				continue
			}
		}
		if a.shouldJury(d) {
			if err := a.openJury(d); err == nil {
				continue
			}
		}
		a.st.SetDisputeStatus(d.ID, "review")
	}
	as, _ := a.st.AppealExpired(now)
	for _, d := range as {
		if err := a.applyResolution(d, d.Resolution, d.ResolutionNote, 0, "", ""); err != nil {
			log.Printf("[error] 上诉期满执行裁决 %s: %v", d.Code, err)
			a.st.SetDisputeStatus(d.ID, "review")
		}
	}
}

// remind 每小时：待确认 24h/72h、付款时限剩 12h。窗口下界用上次执行时间，停机期间的阈值不会漏。
func (a *App) remind(now, since int64) {
	for _, h := range a.cfg.ConfirmRemindH {
		subs, _ := a.st.AwaitingSince(now - h*hourMs)
		for _, x := range subs {
			if max(x.MarkedPaidAt, x.TopupMarkedAt) > since-h*hourMs { // 阈值时刻落在 (since, now] 内的只提醒一次（补差登记会重新起算）
				until := ""
				if a.cfg.AutoConfirmH > h && !(x.TopupRequested > 0 && x.TopupMarkedAt == 0) {
					until = fmt.Sprintf("，%s 后不处理会视为已收到、自动完成", dur(a.cfg.AutoConfirmH-h))
				}
				a.notify(x.WorkerID, "pay", fmt.Sprintf("待确认到账已 %d 小时", h), "记录 "+x.Code+"：请核对后点「已收到」或发起申诉"+until+"。", x.Path())
			}
		}
	}
	subs, _ := a.st.querySubs(`WHERE status='payable' AND pay_deadline_at > ? AND pay_deadline_at <= ?`, since+12*hourMs, now+12*hourMs)
	for _, x := range subs {
		if t, _ := a.st.GetTaskByID(x.TaskID); t != nil {
			a.notify(t.OwnerID, "pay", "付款时限还剩 12 小时", "记录 "+x.Code+"，逾期将被冻结。", x.Path())
		}
	}
}

// backup SQLite 在线备份，保留 14 天。
func (a *App) backup() {
	dir := filepath.Join(filepath.Dir(a.cfg.DBPath), "backups")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	name := filepath.Join(dir, "xjobclub-"+time.Now().Format("20060102")+".db")
	if _, err := a.st.db.Exec(`VACUUM INTO ?`, name); err != nil {
		log.Printf("[warn] 备份失败: %v", err)
		return
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if info, err := e.Info(); err == nil && time.Since(info.ModTime()) > 14*24*time.Hour {
			os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}

// autoApproveCheckings 点赞/转发待核对超过 48 小时发布方未处理：视为通过，进入待付款。
func (a *App) autoApproveCheckings(now int64) {
	list, _ := a.st.DueCheckings(now - checkWindowMs)
	for _, x := range list {
		t, _ := a.st.GetTaskByID(x.TaskID)
		if t == nil {
			continue
		}
		if ok, _ := a.st.SetCheckedOK(x.ID, []string{SChecking}, now+t.PayWindowH*hourMs, 1); !ok {
			continue
		}
		a.st.Audit(0, "sub.check_auto", "submission", x.ID, nil, "")
		a.notify(x.WorkerID, "verify", "发布方超时未核对，视为通过", fmt.Sprintf("发布方须在 %s 内付款 %s U。", dur(t.PayWindowH), fmtE8(payAmount(x, t))), x.Path())
		a.notify(t.OwnerID, "pay", "待核对超时视为通过，请付款", fmt.Sprintf("《%s》有一条%s记录 48 小时未核对，已进入待付款，请在 %s 内付款 %s U。确实没完成的话可在记录页发起申诉。", t.Title, t.DoneVerb(), dur(t.PayWindowH), fmtE8(payAmount(x, t))), x.Path())
	}
}
