USE [{vDB}]
GO
/****** Object:  Table [dbo].[def_vehic]    Script Date: 19/04/2017 3:14:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO

IF OBJECT_ID('dbo.def_vehic', 'U') IS NOT NULL 
  DROP TABLE [dbo].[def_vehic]; 


CREATE TABLE [dbo].[def_vehic](
	[ack_timer] [int] NOT NULL,
	[ag_id] [varchar](9) NOT NULL,
	[arrive_timer] [int] NOT NULL,
	[carid] [varchar](12) NOT NULL,
	[delay_value] [int] NOT NULL,
	[disp_timer] [int] NOT NULL,
	[enroute_timer] [int] NOT NULL,
	[max_ht] [int] NOT NULL,
	[max_wd] [int] NOT NULL,
	[max_wt] [int] NOT NULL,
	[page_id] [varchar](12) NULL,
	[page_unit_person] [smallint] NOT NULL,
	[ras_surefire_sync] [varchar](1) NOT NULL,
	[vflag] [varchar](1) NOT NULL,
 CONSTRAINT [def_vehic_primary_key] PRIMARY KEY CLUSTERED 
(
	[carid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [dbo].[def_vehic] ADD  CONSTRAINT [def_vehic_ack_timer_default]  DEFAULT ((0)) FOR [ack_timer]
GO
ALTER TABLE [dbo].[def_vehic] ADD  CONSTRAINT [def_vehic_arrive_timer_default]  DEFAULT ((0)) FOR [arrive_timer]
GO
ALTER TABLE [dbo].[def_vehic] ADD  CONSTRAINT [def_vehic_delay_value_default]  DEFAULT ((0)) FOR [delay_value]
GO
ALTER TABLE [dbo].[def_vehic] ADD  CONSTRAINT [def_vehic_disp_timer_default]  DEFAULT ((0)) FOR [disp_timer]
GO
ALTER TABLE [dbo].[def_vehic] ADD  CONSTRAINT [def_vehic_enroute_timer_default]  DEFAULT ((0)) FOR [enroute_timer]
GO
ALTER TABLE [dbo].[def_vehic] ADD  CONSTRAINT [def_vehic_max_ht_default]  DEFAULT ((0)) FOR [max_ht]
GO
ALTER TABLE [dbo].[def_vehic] ADD  CONSTRAINT [def_vehic_max_wd_default]  DEFAULT ((0)) FOR [max_wd]
GO
ALTER TABLE [dbo].[def_vehic] ADD  CONSTRAINT [def_vehic_max_wt_default]  DEFAULT ((0)) FOR [max_wt]
GO
ALTER TABLE [dbo].[def_vehic] ADD  CONSTRAINT [def_vehic_page_unit_person_default]  DEFAULT ((0)) FOR [page_unit_person]
GO
ALTER TABLE [dbo].[def_vehic] ADD  CONSTRAINT [def_vehic_ras_surefire_sync_default]  DEFAULT ('N') FOR [ras_surefire_sync]
GO
ALTER TABLE [dbo].[def_vehic] ADD  CONSTRAINT [def_vehic_vflag_default]  DEFAULT ('Y') FOR [vflag]
GO
ALTER TABLE [dbo].[def_vehic]  WITH CHECK ADD  CONSTRAINT [def_vehic_ack_timer_ck] CHECK  (([ack_timer]>=(0)))
GO
ALTER TABLE [dbo].[def_vehic] CHECK CONSTRAINT [def_vehic_ack_timer_ck]
GO
ALTER TABLE [dbo].[def_vehic]  WITH CHECK ADD  CONSTRAINT [def_vehic_arrive_timer_ck] CHECK  (([arrive_timer]>=(0)))
GO
ALTER TABLE [dbo].[def_vehic] CHECK CONSTRAINT [def_vehic_arrive_timer_ck]
GO
ALTER TABLE [dbo].[def_vehic]  WITH CHECK ADD  CONSTRAINT [def_vehic_delay_value_ck] CHECK  (([delay_value]>=(0)))
GO
ALTER TABLE [dbo].[def_vehic] CHECK CONSTRAINT [def_vehic_delay_value_ck]
GO
ALTER TABLE [dbo].[def_vehic]  WITH CHECK ADD  CONSTRAINT [def_vehic_disp_timer_ck] CHECK  (([disp_timer]>=(0)))
GO
ALTER TABLE [dbo].[def_vehic] CHECK CONSTRAINT [def_vehic_disp_timer_ck]
GO
ALTER TABLE [dbo].[def_vehic]  WITH CHECK ADD  CONSTRAINT [def_vehic_enroute_timer_ck] CHECK  (([enroute_timer]>=(0)))
GO
ALTER TABLE [dbo].[def_vehic] CHECK CONSTRAINT [def_vehic_enroute_timer_ck]
GO
ALTER TABLE [dbo].[def_vehic]  WITH CHECK ADD  CONSTRAINT [def_vehic_max_ht_ck] CHECK  (([max_ht]>=(0)))
GO
ALTER TABLE [dbo].[def_vehic] CHECK CONSTRAINT [def_vehic_max_ht_ck]
GO
ALTER TABLE [dbo].[def_vehic]  WITH CHECK ADD  CONSTRAINT [def_vehic_max_wd_ck] CHECK  (([max_wd]>=(0)))
GO
ALTER TABLE [dbo].[def_vehic] CHECK CONSTRAINT [def_vehic_max_wd_ck]
GO
ALTER TABLE [dbo].[def_vehic]  WITH CHECK ADD  CONSTRAINT [def_vehic_max_wt_ck] CHECK  (([max_wt]>=(0)))
GO
ALTER TABLE [dbo].[def_vehic] CHECK CONSTRAINT [def_vehic_max_wt_ck]
GO
ALTER TABLE [dbo].[def_vehic]  WITH CHECK ADD  CONSTRAINT [def_vehic_page_unit_person_ck] CHECK  (([page_unit_person]=(2) OR [page_unit_person]=(1) OR [page_unit_person]=(0)))
GO
ALTER TABLE [dbo].[def_vehic] CHECK CONSTRAINT [def_vehic_page_unit_person_ck]
GO
ALTER TABLE [dbo].[def_vehic]  WITH CHECK ADD  CONSTRAINT [def_vehic_ras_surefire_sync_ck] CHECK  (([ras_surefire_sync]='N' OR [ras_surefire_sync]='Y'))
GO
ALTER TABLE [dbo].[def_vehic] CHECK CONSTRAINT [def_vehic_ras_surefire_sync_ck]
GO
ALTER TABLE [dbo].[def_vehic]  WITH CHECK ADD  CONSTRAINT [def_vehic_vflag_ck] CHECK  (([vflag]='N' OR [vflag]='Y'))
GO
ALTER TABLE [dbo].[def_vehic] CHECK CONSTRAINT [def_vehic_vflag_ck]
GO
