USE [{vDB}]
GO
/****** Object:  Table [dbo].[out_of_service_type]    Script Date: 19/04/2017 3:14:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO

IF OBJECT_ID('dbo.out_of_service_type', 'U') IS NOT NULL 
  DROP TABLE [dbo].[out_of_service_type]; 


CREATE TABLE [dbo].[out_of_service_type](
	[can_dispatch] [varchar](1) NOT NULL,
	[eng] [varchar](80) NULL,
	[tycod] [varchar](16) NOT NULL,
 CONSTRAINT [out_of_service_type_pk] PRIMARY KEY CLUSTERED 
(
	[tycod] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [dbo].[out_of_service_type] ADD  CONSTRAINT [out_of_service_type_can_dispatch_default]  DEFAULT ('F') FOR [can_dispatch]
GO
ALTER TABLE [dbo].[out_of_service_type]  WITH CHECK ADD  CONSTRAINT [out_of_service_can_disp_ck] CHECK  (([can_dispatch]='F' OR [can_dispatch]='T'))
GO
ALTER TABLE [dbo].[out_of_service_type] CHECK CONSTRAINT [out_of_service_can_disp_ck]
GO
