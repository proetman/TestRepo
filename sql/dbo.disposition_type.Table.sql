USE [{vDB}]
GO
/****** Object:  Table [dbo].[disposition_type]    Script Date: 19/04/2017 3:14:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO

IF OBJECT_ID('dbo.disposition_type', 'U') IS NOT NULL 
  DROP TABLE [dbo].[disposition_type]; 


CREATE TABLE [dbo].[disposition_type](
	[ag_id] [varchar](9) NOT NULL,
	[case_num_id] [int] NULL,
	[eng] [varchar](80) NULL,
	[tycod] [varchar](16) NOT NULL,
 CONSTRAINT [disposition_type_pk] PRIMARY KEY CLUSTERED 
(
	[ag_id] ASC,
	[tycod] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [dbo].[disposition_type]  WITH CHECK ADD  CONSTRAINT [disposition_type_case_num_ck] CHECK  (([case_num_id]>(0)))
GO
ALTER TABLE [dbo].[disposition_type] CHECK CONSTRAINT [disposition_type_case_num_ck]
GO
