USE [AdventureWorks2012]
GO
/****** Object:  Table [dbo].[event_type_alarm_level]    Script Date: 19/04/2017 3:14:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[event_type_alarm_level](
	[additional_events] [varchar](80) NULL,
	[ag_id] [varchar](9) NOT NULL,
	[alarm_lev] [int] NOT NULL,
	[fs_icode] [varchar](4) NULL,
	[fs_icodemessage] [varchar](50) NULL,
	[fs_rcode] [varchar](4) NULL,
	[fs_rcodemessage] [varchar](60) NULL,
	[intercadsend] [varchar](1) NULL,
	[msg] [varchar](80) NULL,
	[page_id] [varchar](12) NULL,
	[resp_plan_name] [varchar](50) NULL,
	[sub_tycod] [varchar](16) NOT NULL,
	[tycod] [varchar](16) NOT NULL,
 CONSTRAINT [event_type_alarm_level_pk] PRIMARY KEY CLUSTERED 
(
	[tycod] ASC,
	[sub_tycod] ASC,
	[alarm_lev] ASC,
	[ag_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [dbo].[event_type_alarm_level] ADD  CONSTRAINT [event_type_alarm_level_alarm_lev_default]  DEFAULT ((0)) FOR [alarm_lev]
GO
ALTER TABLE [dbo].[event_type_alarm_level] ADD  CONSTRAINT [event_type_alarm_level_sub_tycod_default]  DEFAULT ('default') FOR [sub_tycod]
GO
ALTER TABLE [dbo].[event_type_alarm_level]  WITH CHECK ADD  CONSTRAINT [event_type_alarm_lev_ck] CHECK  (([alarm_lev]>=(0)))
GO
ALTER TABLE [dbo].[event_type_alarm_level] CHECK CONSTRAINT [event_type_alarm_lev_ck]
GO
