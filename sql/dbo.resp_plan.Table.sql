USE [{vDB}]
GO
/****** Object:  Table [dbo].[resp_plan]    Script Date: 19/04/2017 3:14:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO

IF OBJECT_ID('dbo.resp_plan', 'U') IS NOT NULL 
  DROP TABLE [dbo].[resp_plan]; 


CREATE TABLE [dbo].[resp_plan](
	[alt_unit_count] [int] NOT NULL,
	[comm] [varchar](80) NULL,
	[cond_type] [int] NOT NULL,
	[failure_item_id] [int] NOT NULL,
	[from_day] [smallint] NOT NULL,
	[from_hour] [smallint] NOT NULL,
	[from_minute] [smallint] NOT NULL,
	[from_month] [smallint] NOT NULL,
	[from_weekday] [smallint] NOT NULL,
	[item_col] [int] NOT NULL,
	[item_id] [int] NOT NULL,
	[item_row] [int] NOT NULL,
	[item_type] [int] NOT NULL,
	[plan_ref] [varchar](50) NULL,
	[recommend_mode] [int] NOT NULL,
	[req_descrip] [varchar](50) NULL,
	[req_display_order] [int] NOT NULL,
	[req_max_beat] [int] NOT NULL,
	[req_max_route] [float] NOT NULL,
	[req_name] [varchar](50) NULL,
	[req_number] [int] NOT NULL,
	[req_quan] [int] NOT NULL,
	[resp_plan_name] [varchar](50) NOT NULL,
	[resp_type] [int] NOT NULL,
	[success_item_id] [int] NOT NULL,
	[to_day] [smallint] NOT NULL,
	[to_hour] [smallint] NOT NULL,
	[to_minute] [smallint] NOT NULL,
	[to_month] [smallint] NOT NULL,
	[to_weekday] [smallint] NOT NULL,
	[unique_id] [varbinary](16) NOT NULL,
 CONSTRAINT [resp_plan_primary_key] PRIMARY KEY CLUSTERED 
(
	[resp_plan_name] ASC,
	[item_id] ASC,
	[req_number] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [resp_plan_unique] UNIQUE NONCLUSTERED 
(
	[unique_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [dbo].[resp_plan] ADD  CONSTRAINT [resp_plan_alt_unit_count_default]  DEFAULT ((0)) FOR [alt_unit_count]
GO
ALTER TABLE [dbo].[resp_plan] ADD  CONSTRAINT [resp_plan_cond_type_default]  DEFAULT ((0)) FOR [cond_type]
GO
ALTER TABLE [dbo].[resp_plan] ADD  CONSTRAINT [resp_plan_from_day_default]  DEFAULT ((0)) FOR [from_day]
GO
ALTER TABLE [dbo].[resp_plan] ADD  CONSTRAINT [resp_plan_from_hour_default]  DEFAULT ((24)) FOR [from_hour]
GO
ALTER TABLE [dbo].[resp_plan] ADD  CONSTRAINT [resp_plan_from_minute_default]  DEFAULT ((0)) FOR [from_minute]
GO
ALTER TABLE [dbo].[resp_plan] ADD  CONSTRAINT [resp_plan_from_month_default]  DEFAULT ((0)) FOR [from_month]
GO
ALTER TABLE [dbo].[resp_plan] ADD  CONSTRAINT [resp_plan_from_weekday_default]  DEFAULT ((7)) FOR [from_weekday]
GO
ALTER TABLE [dbo].[resp_plan] ADD  CONSTRAINT [resp_plan_recommend_mode_default]  DEFAULT ((3)) FOR [recommend_mode]
GO
ALTER TABLE [dbo].[resp_plan] ADD  CONSTRAINT [resp_plan_req_display_order_default]  DEFAULT ((0)) FOR [req_display_order]
GO
ALTER TABLE [dbo].[resp_plan] ADD  CONSTRAINT [resp_plan_req_max_beat_default]  DEFAULT ((0)) FOR [req_max_beat]
GO
ALTER TABLE [dbo].[resp_plan] ADD  CONSTRAINT [resp_plan_req_max_route_default]  DEFAULT ((0)) FOR [req_max_route]
GO
ALTER TABLE [dbo].[resp_plan] ADD  CONSTRAINT [resp_plan_req_quan_default]  DEFAULT ((0)) FOR [req_quan]
GO
ALTER TABLE [dbo].[resp_plan] ADD  CONSTRAINT [resp_plan_resp_type_default]  DEFAULT ((0)) FOR [resp_type]
GO
ALTER TABLE [dbo].[resp_plan] ADD  CONSTRAINT [resp_plan_to_day_default]  DEFAULT ((0)) FOR [to_day]
GO
ALTER TABLE [dbo].[resp_plan] ADD  CONSTRAINT [resp_plan_to_hour_default]  DEFAULT ((24)) FOR [to_hour]
GO
ALTER TABLE [dbo].[resp_plan] ADD  CONSTRAINT [resp_plan_to_minute_default]  DEFAULT ((0)) FOR [to_minute]
GO
ALTER TABLE [dbo].[resp_plan] ADD  CONSTRAINT [resp_plan_to_month_default]  DEFAULT ((0)) FOR [to_month]
GO
ALTER TABLE [dbo].[resp_plan] ADD  CONSTRAINT [resp_plan_to_weekday_default]  DEFAULT ((7)) FOR [to_weekday]
GO
ALTER TABLE [dbo].[resp_plan]  WITH CHECK ADD  CONSTRAINT [resp_plan_cond_type_ck] CHECK  (([cond_type]>=(0) AND [cond_type]<=(2)))
GO
ALTER TABLE [dbo].[resp_plan] CHECK CONSTRAINT [resp_plan_cond_type_ck]
GO
ALTER TABLE [dbo].[resp_plan]  WITH CHECK ADD  CONSTRAINT [resp_plan_from_day_ck] CHECK  (([from_day]>=(0) AND [from_day]<=(31)))
GO
ALTER TABLE [dbo].[resp_plan] CHECK CONSTRAINT [resp_plan_from_day_ck]
GO
ALTER TABLE [dbo].[resp_plan]  WITH CHECK ADD  CONSTRAINT [resp_plan_from_hour_ck] CHECK  (([from_hour]>=(0) AND [from_hour]<=(24)))
GO
ALTER TABLE [dbo].[resp_plan] CHECK CONSTRAINT [resp_plan_from_hour_ck]
GO
ALTER TABLE [dbo].[resp_plan]  WITH CHECK ADD  CONSTRAINT [resp_plan_from_minute_ck] CHECK  (([from_minute]>=(0) AND [from_minute]<=(59)))
GO
ALTER TABLE [dbo].[resp_plan] CHECK CONSTRAINT [resp_plan_from_minute_ck]
GO
ALTER TABLE [dbo].[resp_plan]  WITH CHECK ADD  CONSTRAINT [resp_plan_from_month_ck] CHECK  (([from_month]>=(0) AND [from_month]<=(12)))
GO
ALTER TABLE [dbo].[resp_plan] CHECK CONSTRAINT [resp_plan_from_month_ck]
GO
ALTER TABLE [dbo].[resp_plan]  WITH CHECK ADD  CONSTRAINT [resp_plan_from_weekday_ck] CHECK  (([from_weekday]>=(0) AND [from_weekday]<=(7)))
GO
ALTER TABLE [dbo].[resp_plan] CHECK CONSTRAINT [resp_plan_from_weekday_ck]
GO
ALTER TABLE [dbo].[resp_plan]  WITH CHECK ADD  CONSTRAINT [resp_plan_item_type_ck] CHECK  (([item_type]>=(0) AND [item_type]<=(3)))
GO
ALTER TABLE [dbo].[resp_plan] CHECK CONSTRAINT [resp_plan_item_type_ck]
GO
ALTER TABLE [dbo].[resp_plan]  WITH CHECK ADD  CONSTRAINT [resp_plan_recommend_mode_ck] CHECK  (([recommend_mode]=(3) OR [recommend_mode]=(1) OR [recommend_mode]=(0)))
GO
ALTER TABLE [dbo].[resp_plan] CHECK CONSTRAINT [resp_plan_recommend_mode_ck]
GO
ALTER TABLE [dbo].[resp_plan]  WITH CHECK ADD  CONSTRAINT [resp_plan_req_number_ck] CHECK  (([req_number]>=(0)))
GO
ALTER TABLE [dbo].[resp_plan] CHECK CONSTRAINT [resp_plan_req_number_ck]
GO
ALTER TABLE [dbo].[resp_plan]  WITH CHECK ADD  CONSTRAINT [resp_plan_resp_type_ck] CHECK  (([resp_type]=(1) OR [resp_type]=(0)))
GO
ALTER TABLE [dbo].[resp_plan] CHECK CONSTRAINT [resp_plan_resp_type_ck]
GO
ALTER TABLE [dbo].[resp_plan]  WITH CHECK ADD  CONSTRAINT [resp_plan_to_day_ck] CHECK  (([to_day]>=(0) AND [to_day]<=(31)))
GO
ALTER TABLE [dbo].[resp_plan] CHECK CONSTRAINT [resp_plan_to_day_ck]
GO
ALTER TABLE [dbo].[resp_plan]  WITH CHECK ADD  CONSTRAINT [resp_plan_to_hour_ck] CHECK  (([to_hour]>=(0) AND [to_hour]<=(24)))
GO
ALTER TABLE [dbo].[resp_plan] CHECK CONSTRAINT [resp_plan_to_hour_ck]
GO
ALTER TABLE [dbo].[resp_plan]  WITH CHECK ADD  CONSTRAINT [resp_plan_to_minute_ck] CHECK  (([to_minute]>=(0) AND [to_minute]<=(59)))
GO
ALTER TABLE [dbo].[resp_plan] CHECK CONSTRAINT [resp_plan_to_minute_ck]
GO
ALTER TABLE [dbo].[resp_plan]  WITH CHECK ADD  CONSTRAINT [resp_plan_to_month_ck] CHECK  (([to_month]>=(0) AND [to_month]<=(12)))
GO
ALTER TABLE [dbo].[resp_plan] CHECK CONSTRAINT [resp_plan_to_month_ck]
GO
ALTER TABLE [dbo].[resp_plan]  WITH CHECK ADD  CONSTRAINT [resp_plan_to_weekday_ck] CHECK  (([to_weekday]>=(0) AND [to_weekday]<=(7)))
GO
ALTER TABLE [dbo].[resp_plan] CHECK CONSTRAINT [resp_plan_to_weekday_ck]
GO
