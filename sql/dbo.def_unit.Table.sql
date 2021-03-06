USE [{vDB}]
GO
/****** Object:  Table [dbo].[def_unit]    Script Date: 19/04/2017 3:14:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO

IF OBJECT_ID('dbo.def_unit', 'U') IS NOT NULL 
  DROP TABLE [dbo].[def_unit]; 

CREATE TABLE [dbo].[def_unit](
	[ag_id] [varchar](9) NOT NULL,
	[bay] [varchar](3) NULL,
	[break_duration] [smallint] NULL,
	[break_earliest_time] [varchar](16) NULL,
	[break_latest_time] [varchar](16) NULL,
	[break_start_time] [varchar](16) NULL,
	[break_taken] [varchar](1) NOT NULL,
	[dgroup] [varchar](5) NOT NULL,
	[end_shift_loc_x] [int] NULL,
	[end_shift_loc_y] [int] NULL,
	[ext_unitattr] [varbinary](32) NOT NULL,
	[hourly_cost_sp] [float] NULL,
	[lev3] [varchar](7) NULL,
	[loc_comm] [varchar](40) NULL,
	[loc_x] [float] NOT NULL,
	[loc_y] [float] NOT NULL,
	[max_disp_assign_jobs] [smallint] NULL,
	[max_time_startpt] [int] NULL,
	[mdtgroup] [varchar](16) NULL,
	[overtime_cost] [float] NULL,
	[per_job_cost_sp] [float] NULL,
	[ras_group_id] [int] NULL,
	[ras_mealbreak_type] [varchar](5) NOT NULL,
	[ras_supplier_id] [int] NULL,
	[require_cor] [varchar](1) NOT NULL,
	[resp_stop] [varchar](1) NOT NULL,
	[shared_crew] [varchar](1) NOT NULL,
	[shift_overtime] [float] NULL,
	[start_shift_loc_x] [int] NULL,
	[start_shift_loc_y] [int] NULL,
	[station] [varchar](10) NULL,
	[symnum] [int] NOT NULL,
	[travel_cost] [float] NULL,
	[type_avail] [smallint] NOT NULL,
	[ucust1] [varchar](50) NULL,
	[ucust2] [varchar](50) NULL,
	[ucust3] [varchar](50) NULL,
	[ucust4] [varchar](50) NULL,
	[unid] [varchar](10) NOT NULL,
	[unit_status] [smallint] NOT NULL,
	[unityp] [varchar](6) NULL,
	[wait_cost] [float] NULL,
 CONSTRAINT [def_unit_primary_key] PRIMARY KEY CLUSTERED 
(
	[unid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [dbo].[def_unit] ADD  CONSTRAINT [def_unit_break_taken_default]  DEFAULT ('0') FOR [break_taken]
GO
ALTER TABLE [dbo].[def_unit] ADD  CONSTRAINT [def_unit_ext_unitattr_default]  DEFAULT (0x0000000000000000000000000000000000000000000000000000000000000000) FOR [ext_unitattr]
GO
ALTER TABLE [dbo].[def_unit] ADD  CONSTRAINT [def_unit_loc_x_default]  DEFAULT ((0.0)) FOR [loc_x]
GO
ALTER TABLE [dbo].[def_unit] ADD  CONSTRAINT [def_unit_loc_y_default]  DEFAULT ((0.0)) FOR [loc_y]
GO
ALTER TABLE [dbo].[def_unit] ADD  CONSTRAINT [def_unit_ras_mealbreak_type_default]  DEFAULT ('NONE') FOR [ras_mealbreak_type]
GO
ALTER TABLE [dbo].[def_unit] ADD  CONSTRAINT [def_unit_require_cor_default]  DEFAULT ('0') FOR [require_cor]
GO
ALTER TABLE [dbo].[def_unit] ADD  CONSTRAINT [def_unit_resp_stop_default]  DEFAULT ('N') FOR [resp_stop]
GO
ALTER TABLE [dbo].[def_unit] ADD  CONSTRAINT [def_unit_shared_crew_default]  DEFAULT ('N') FOR [shared_crew]
GO
ALTER TABLE [dbo].[def_unit] ADD  CONSTRAINT [def_unit_symnum_default]  DEFAULT ((0)) FOR [symnum]
GO
ALTER TABLE [dbo].[def_unit] ADD  CONSTRAINT [def_unit_type_avail_default]  DEFAULT ((0)) FOR [type_avail]
GO
ALTER TABLE [dbo].[def_unit] ADD  CONSTRAINT [def_unit_unit_status_default]  DEFAULT ((0)) FOR [unit_status]
GO
ALTER TABLE [dbo].[def_unit]  WITH CHECK ADD  CONSTRAINT [def_unit_break_duration_ck] CHECK  (([break_duration] IS NULL OR [break_duration]>=(0)))
GO
ALTER TABLE [dbo].[def_unit] CHECK CONSTRAINT [def_unit_break_duration_ck]
GO
ALTER TABLE [dbo].[def_unit]  WITH CHECK ADD  CONSTRAINT [def_unit_break_earlies_time_ck] CHECK  (([break_earliest_time] like '[1-9][0-9][0-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9][0-5][0-9][A-Z][A-Z]' AND CONVERT([int],substring([break_earliest_time],(1),(4)))>=(1970) AND ((substring([break_earliest_time],(5),(2))='12' OR substring([break_earliest_time],(5),(2))='10' OR substring([break_earliest_time],(5),(2))='08' OR substring([break_earliest_time],(5),(2))='07' OR substring([break_earliest_time],(5),(2))='05' OR substring([break_earliest_time],(5),(2))='03' OR substring([break_earliest_time],(5),(2))='01') AND CONVERT([int],substring([break_earliest_time],(7),(2)))<=(31) OR (substring([break_earliest_time],(5),(2))='11' OR substring([break_earliest_time],(5),(2))='09' OR substring([break_earliest_time],(5),(2))='06' OR substring([break_earliest_time],(5),(2))='04') AND CONVERT([int],substring([break_earliest_time],(7),(2)))<=(30) OR substring([break_earliest_time],(5),(2))='02' AND CONVERT([int],substring([break_earliest_time],(7),(2)))<=(29)) AND CONVERT([int],substring([break_earliest_time],(9),(2)))<=(24) AND substring([break_earliest_time],(15),(2))='UT' AND (NOT [break_earliest_time] like '[0-9][0-9][0-9][0-9]0229%' OR CONVERT([int],substring([break_earliest_time],(1),(4)))%(4)=(0) AND (CONVERT([int],substring([break_earliest_time],(1),(4)))%(100)<>(0) OR CONVERT([int],substring([break_earliest_time],(1),(4)))%(400)=(0)))))
GO
ALTER TABLE [dbo].[def_unit] CHECK CONSTRAINT [def_unit_break_earlies_time_ck]
GO
ALTER TABLE [dbo].[def_unit]  WITH CHECK ADD  CONSTRAINT [def_unit_break_latest_time_ck] CHECK  (([break_latest_time] like '[1-9][0-9][0-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9][0-5][0-9][A-Z][A-Z]' AND CONVERT([int],substring([break_latest_time],(1),(4)))>=(1970) AND ((substring([break_latest_time],(5),(2))='12' OR substring([break_latest_time],(5),(2))='10' OR substring([break_latest_time],(5),(2))='08' OR substring([break_latest_time],(5),(2))='07' OR substring([break_latest_time],(5),(2))='05' OR substring([break_latest_time],(5),(2))='03' OR substring([break_latest_time],(5),(2))='01') AND CONVERT([int],substring([break_latest_time],(7),(2)))<=(31) OR (substring([break_latest_time],(5),(2))='11' OR substring([break_latest_time],(5),(2))='09' OR substring([break_latest_time],(5),(2))='06' OR substring([break_latest_time],(5),(2))='04') AND CONVERT([int],substring([break_latest_time],(7),(2)))<=(30) OR substring([break_latest_time],(5),(2))='02' AND CONVERT([int],substring([break_latest_time],(7),(2)))<=(29)) AND CONVERT([int],substring([break_latest_time],(9),(2)))<=(24) AND substring([break_latest_time],(15),(2))='UT' AND (NOT [break_latest_time] like '[0-9][0-9][0-9][0-9]0229%' OR CONVERT([int],substring([break_latest_time],(1),(4)))%(4)=(0) AND (CONVERT([int],substring([break_latest_time],(1),(4)))%(100)<>(0) OR CONVERT([int],substring([break_latest_time],(1),(4)))%(400)=(0)))))
GO
ALTER TABLE [dbo].[def_unit] CHECK CONSTRAINT [def_unit_break_latest_time_ck]
GO
ALTER TABLE [dbo].[def_unit]  WITH CHECK ADD  CONSTRAINT [def_unit_break_start_time_ck] CHECK  (([break_start_time] like '[1-9][0-9][0-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9][0-5][0-9][A-Z][A-Z]' AND CONVERT([int],substring([break_start_time],(1),(4)))>=(1970) AND ((substring([break_start_time],(5),(2))='12' OR substring([break_start_time],(5),(2))='10' OR substring([break_start_time],(5),(2))='08' OR substring([break_start_time],(5),(2))='07' OR substring([break_start_time],(5),(2))='05' OR substring([break_start_time],(5),(2))='03' OR substring([break_start_time],(5),(2))='01') AND CONVERT([int],substring([break_start_time],(7),(2)))<=(31) OR (substring([break_start_time],(5),(2))='11' OR substring([break_start_time],(5),(2))='09' OR substring([break_start_time],(5),(2))='06' OR substring([break_start_time],(5),(2))='04') AND CONVERT([int],substring([break_start_time],(7),(2)))<=(30) OR substring([break_start_time],(5),(2))='02' AND CONVERT([int],substring([break_start_time],(7),(2)))<=(29)) AND CONVERT([int],substring([break_start_time],(9),(2)))<=(24) AND substring([break_start_time],(15),(2))='UT' AND (NOT [break_start_time] like '[0-9][0-9][0-9][0-9]0229%' OR CONVERT([int],substring([break_start_time],(1),(4)))%(4)=(0) AND (CONVERT([int],substring([break_start_time],(1),(4)))%(100)<>(0) OR CONVERT([int],substring([break_start_time],(1),(4)))%(400)=(0)))))
GO
ALTER TABLE [dbo].[def_unit] CHECK CONSTRAINT [def_unit_break_start_time_ck]
GO
ALTER TABLE [dbo].[def_unit]  WITH CHECK ADD  CONSTRAINT [def_unit_break_taken_ck] CHECK  (([break_taken]='0' OR [break_taken]='1'))
GO
ALTER TABLE [dbo].[def_unit] CHECK CONSTRAINT [def_unit_break_taken_ck]
GO
ALTER TABLE [dbo].[def_unit]  WITH CHECK ADD  CONSTRAINT [def_unit_ext_unitattr_ck] CHECK  ((datalength([ext_unitattr])=(32)))
GO
ALTER TABLE [dbo].[def_unit] CHECK CONSTRAINT [def_unit_ext_unitattr_ck]
GO
ALTER TABLE [dbo].[def_unit]  WITH CHECK ADD  CONSTRAINT [def_unit_max_disp_assn_jobs_ck] CHECK  (([max_disp_assign_jobs] IS NULL OR [max_disp_assign_jobs]>=(0)))
GO
ALTER TABLE [dbo].[def_unit] CHECK CONSTRAINT [def_unit_max_disp_assn_jobs_ck]
GO
ALTER TABLE [dbo].[def_unit]  WITH CHECK ADD  CONSTRAINT [def_unit_max_time_startpt_ck] CHECK  (([max_time_startpt] IS NULL OR [max_time_startpt]>=(0)))
GO
ALTER TABLE [dbo].[def_unit] CHECK CONSTRAINT [def_unit_max_time_startpt_ck]
GO
ALTER TABLE [dbo].[def_unit]  WITH CHECK ADD  CONSTRAINT [def_unit_ras_mealbreak_type_ck] CHECK  (([ras_mealbreak_type]='FIELD' OR [ras_mealbreak_type]='CRIB' OR [ras_mealbreak_type]='NONE'))
GO
ALTER TABLE [dbo].[def_unit] CHECK CONSTRAINT [def_unit_ras_mealbreak_type_ck]
GO
ALTER TABLE [dbo].[def_unit]  WITH CHECK ADD  CONSTRAINT [def_unit_require_cor_ck] CHECK  (([require_cor]='0' OR [require_cor]='1'))
GO
ALTER TABLE [dbo].[def_unit] CHECK CONSTRAINT [def_unit_require_cor_ck]
GO
ALTER TABLE [dbo].[def_unit]  WITH CHECK ADD  CONSTRAINT [def_unit_resp_stop_ck] CHECK  (([resp_stop]='N' OR [resp_stop]='Y'))
GO
ALTER TABLE [dbo].[def_unit] CHECK CONSTRAINT [def_unit_resp_stop_ck]
GO
ALTER TABLE [dbo].[def_unit]  WITH CHECK ADD  CONSTRAINT [def_unit_shared_crew_ck] CHECK  (([shared_crew]='N' OR [shared_crew]='Y'))
GO
ALTER TABLE [dbo].[def_unit] CHECK CONSTRAINT [def_unit_shared_crew_ck]
GO
ALTER TABLE [dbo].[def_unit]  WITH CHECK ADD  CONSTRAINT [def_unit_symnum_ck] CHECK  (([symnum]>=(0) AND [symnum]<=(255)))
GO
ALTER TABLE [dbo].[def_unit] CHECK CONSTRAINT [def_unit_symnum_ck]
GO
