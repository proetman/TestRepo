USE [{vDB}]
GO
/****** Object:  Table [dbo].[event_type]    Script Date: 19/04/2017 3:14:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO

IF OBJECT_ID('dbo.event_type', 'U') IS NOT NULL 
  DROP TABLE [dbo].[event_type]; 



CREATE TABLE [dbo].[event_type](
	[acknowledge_timer] [int] NOT NULL,
	[advised_event] [varchar](1) NOT NULL,
	[ag_id] [varchar](9) NOT NULL,
	[arrive_timer] [int] NOT NULL,
	[assign_case_state] [int] NOT NULL,
	[auto_response] [varchar](1) NOT NULL,
	[case_num_id] [int] NULL,
	[days_avail] [int] NOT NULL,
	[disp_req] [varchar](1) NOT NULL,
	[dispatch_timer] [int] NOT NULL,
	[eng] [varchar](80) NULL,
	[enroute_timer] [int] NOT NULL,
	[initial_callback_timer] [int] NOT NULL,
	[loi_dist] [real] NULL,
	[majevt_id] [varchar](10) NULL,
	[near_dist] [real] NULL,
	[pending_timer] [int] NOT NULL,
	[pending_timer_default] [int] NOT NULL,
	[priority] [int] NOT NULL,
	[repeat_callback_timer] [int] NOT NULL,
	[sub_eng] [varchar](80) NULL,
	[sub_tycod] [varchar](16) NOT NULL,
	[tycod] [varchar](16) NOT NULL,
 CONSTRAINT [event_type_pk] PRIMARY KEY CLUSTERED 
(
	[ag_id] ASC,
	[tycod] ASC,
	[sub_tycod] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [dbo].[event_type] ADD  CONSTRAINT [event_type_acknowledge_timer_default]  DEFAULT ((0)) FOR [acknowledge_timer]
GO
ALTER TABLE [dbo].[event_type] ADD  CONSTRAINT [event_type_advised_event_default]  DEFAULT ('F') FOR [advised_event]
GO
ALTER TABLE [dbo].[event_type] ADD  CONSTRAINT [event_type_arrive_timer_default]  DEFAULT ((0)) FOR [arrive_timer]
GO
ALTER TABLE [dbo].[event_type] ADD  CONSTRAINT [event_type_assign_case_state_default]  DEFAULT ((0)) FOR [assign_case_state]
GO
ALTER TABLE [dbo].[event_type] ADD  CONSTRAINT [event_type_auto_response_default]  DEFAULT ('T') FOR [auto_response]
GO
ALTER TABLE [dbo].[event_type] ADD  CONSTRAINT [event_type_days_avail_default]  DEFAULT ((0)) FOR [days_avail]
GO
ALTER TABLE [dbo].[event_type] ADD  CONSTRAINT [event_type_disp_req_default]  DEFAULT ('N') FOR [disp_req]
GO
ALTER TABLE [dbo].[event_type] ADD  CONSTRAINT [event_type_dispatch_timer_default]  DEFAULT ((0)) FOR [dispatch_timer]
GO
ALTER TABLE [dbo].[event_type] ADD  CONSTRAINT [event_type_enroute_timer_default]  DEFAULT ((0)) FOR [enroute_timer]
GO
ALTER TABLE [dbo].[event_type] ADD  CONSTRAINT [event_type_initial_callback_timer_default]  DEFAULT ((0)) FOR [initial_callback_timer]
GO
ALTER TABLE [dbo].[event_type] ADD  CONSTRAINT [event_type_pending_timer_default]  DEFAULT ((0)) FOR [pending_timer]
GO
ALTER TABLE [dbo].[event_type] ADD  CONSTRAINT [event_type_pending_timer_default_default]  DEFAULT ((0)) FOR [pending_timer_default]
GO
ALTER TABLE [dbo].[event_type] ADD  CONSTRAINT [event_type_priority_default]  DEFAULT ((9)) FOR [priority]
GO
ALTER TABLE [dbo].[event_type] ADD  CONSTRAINT [event_type_repeat_callback_timer_default]  DEFAULT ((0)) FOR [repeat_callback_timer]
GO
ALTER TABLE [dbo].[event_type] ADD  CONSTRAINT [event_type_sub_tycod_default]  DEFAULT ('default') FOR [sub_tycod]
GO
ALTER TABLE [dbo].[event_type]  WITH CHECK ADD  CONSTRAINT [event_type_ack_timer_ck] CHECK  (([acknowledge_timer]>=(0) AND [acknowledge_timer]<=(32766)))
GO
ALTER TABLE [dbo].[event_type] CHECK CONSTRAINT [event_type_ack_timer_ck]
GO
ALTER TABLE [dbo].[event_type]  WITH CHECK ADD  CONSTRAINT [event_type_advised_event_ck] CHECK  (([advised_event]='F' OR [advised_event]='T'))
GO
ALTER TABLE [dbo].[event_type] CHECK CONSTRAINT [event_type_advised_event_ck]
GO
ALTER TABLE [dbo].[event_type]  WITH CHECK ADD  CONSTRAINT [event_type_arrive_timer_ck] CHECK  (([arrive_timer]>=(0) AND [arrive_timer]<=(32766)))
GO
ALTER TABLE [dbo].[event_type] CHECK CONSTRAINT [event_type_arrive_timer_ck]
GO
ALTER TABLE [dbo].[event_type]  WITH CHECK ADD  CONSTRAINT [event_type_assign_case_ck] CHECK  (([assign_case_state]=(64) OR [assign_case_state]=(32) OR [assign_case_state]=(16) OR [assign_case_state]=(8) OR [assign_case_state]=(4) OR [assign_case_state]=(2) OR [assign_case_state]=(0)))
GO
ALTER TABLE [dbo].[event_type] CHECK CONSTRAINT [event_type_assign_case_ck]
GO
ALTER TABLE [dbo].[event_type]  WITH CHECK ADD  CONSTRAINT [event_type_auto_response_ck] CHECK  (([auto_response]='F' OR [auto_response]='T'))
GO
ALTER TABLE [dbo].[event_type] CHECK CONSTRAINT [event_type_auto_response_ck]
GO
ALTER TABLE [dbo].[event_type]  WITH CHECK ADD  CONSTRAINT [event_type_case_num_id_ck] CHECK  (([case_num_id]>(0)))
GO
ALTER TABLE [dbo].[event_type] CHECK CONSTRAINT [event_type_case_num_id_ck]
GO
ALTER TABLE [dbo].[event_type]  WITH CHECK ADD  CONSTRAINT [event_type_days_avail_ck] CHECK  (([days_avail]>=(0)))
GO
ALTER TABLE [dbo].[event_type] CHECK CONSTRAINT [event_type_days_avail_ck]
GO
ALTER TABLE [dbo].[event_type]  WITH CHECK ADD  CONSTRAINT [event_type_disp_req_ck] CHECK  (([disp_req]='N' OR [disp_req]='Y'))
GO
ALTER TABLE [dbo].[event_type] CHECK CONSTRAINT [event_type_disp_req_ck]
GO
ALTER TABLE [dbo].[event_type]  WITH CHECK ADD  CONSTRAINT [event_type_dispatch_timer_ck] CHECK  (([dispatch_timer]>=(0) AND [dispatch_timer]<=(32766)))
GO
ALTER TABLE [dbo].[event_type] CHECK CONSTRAINT [event_type_dispatch_timer_ck]
GO
ALTER TABLE [dbo].[event_type]  WITH CHECK ADD  CONSTRAINT [event_type_enroute_timer_ck] CHECK  (([enroute_timer]>=(0) AND [enroute_timer]<=(32766)))
GO
ALTER TABLE [dbo].[event_type] CHECK CONSTRAINT [event_type_enroute_timer_ck]
GO
ALTER TABLE [dbo].[event_type]  WITH CHECK ADD  CONSTRAINT [event_type_loi_dist_ck] CHECK  (([Loi_dist]>=(0.0) AND [Loi_dist]<=(9999999.0)))
GO
ALTER TABLE [dbo].[event_type] CHECK CONSTRAINT [event_type_loi_dist_ck]
GO
ALTER TABLE [dbo].[event_type]  WITH CHECK ADD  CONSTRAINT [event_type_near_dist_ck] CHECK  (([near_dist]>=(0.0) AND [near_dist]<=(9999999.0)))
GO
ALTER TABLE [dbo].[event_type] CHECK CONSTRAINT [event_type_near_dist_ck]
GO
ALTER TABLE [dbo].[event_type]  WITH CHECK ADD  CONSTRAINT [event_type_pend_timer_def_ck] CHECK  (([pending_timer_default]>=(0) AND [pending_timer_default]<=(32766)))
GO
ALTER TABLE [dbo].[event_type] CHECK CONSTRAINT [event_type_pend_timer_def_ck]
GO
ALTER TABLE [dbo].[event_type]  WITH CHECK ADD  CONSTRAINT [event_type_pending_timer_ck] CHECK  (([pending_timer]>=(0) AND [pending_timer]<=(32766)))
GO
ALTER TABLE [dbo].[event_type] CHECK CONSTRAINT [event_type_pending_timer_ck]
GO
ALTER TABLE [dbo].[event_type]  WITH CHECK ADD  CONSTRAINT [event_type_priority_ck] CHECK  (([priority]>=(0) AND [priority]<=(9)))
GO
ALTER TABLE [dbo].[event_type] CHECK CONSTRAINT [event_type_priority_ck]
GO
ALTER TABLE [dbo].[event_type]  WITH CHECK ADD  CONSTRAINT [event_type_sub_tycod_eng_ck] CHECK  ((NOT ([sub_tycod]='default' AND [sub_eng] IS NOT NULL)))
GO
ALTER TABLE [dbo].[event_type] CHECK CONSTRAINT [event_type_sub_tycod_eng_ck]
GO
ALTER TABLE [dbo].[event_type]  WITH CHECK ADD  CONSTRAINT [event_type_sub_tycod_spaces_ck] CHECK  ((charindex(' ',[sub_tycod])=(0)))
GO
ALTER TABLE [dbo].[event_type] CHECK CONSTRAINT [event_type_sub_tycod_spaces_ck]
GO
ALTER TABLE [dbo].[event_type]  WITH CHECK ADD  CONSTRAINT [event_type_tycod_spaces_ck] CHECK  ((charindex(' ',[tycod])=(0)))
GO
ALTER TABLE [dbo].[event_type] CHECK CONSTRAINT [event_type_tycod_spaces_ck]
GO
