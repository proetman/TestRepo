USE [{vDB}]
GO
/****** Object:  Table [dbo].[out_of_service_type_agency]    Script Date: 19/04/2017 3:14:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO

IF OBJECT_ID('dbo.out_of_service_type_agency', 'U') IS NOT NULL 
  DROP TABLE [dbo].[out_of_service_type_agency]; 


CREATE TABLE [dbo].[out_of_service_type_agency](
	[ag_id] [varchar](9) NOT NULL,
	[alert_timer] [int] NOT NULL,
	[tycod] [varchar](16) NOT NULL,
 CONSTRAINT [out_of_service_type_ag_pk] PRIMARY KEY CLUSTERED 
(
	[ag_id] ASC,
	[tycod] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [dbo].[out_of_service_type_agency] ADD  CONSTRAINT [out_of_service_type_agency_alert_timer_default]  DEFAULT ((0)) FOR [alert_timer]
GO
ALTER TABLE [dbo].[out_of_service_type_agency]  WITH CHECK ADD  CONSTRAINT [out_of_service_type_agalert_ck] CHECK  (([alert_timer]>=(0) AND [alert_timer]<=(480)))
GO
ALTER TABLE [dbo].[out_of_service_type_agency] CHECK CONSTRAINT [out_of_service_type_agalert_ck]
GO
