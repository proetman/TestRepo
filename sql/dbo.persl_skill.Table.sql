USE [{vDB}]
GO
/****** Object:  Table [dbo].[persl_skill]    Script Date: 19/04/2017 3:14:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO

IF OBJECT_ID('dbo.persl_skill', 'U') IS NOT NULL 
  DROP TABLE [dbo].[persl_skill]; 

CREATE TABLE [dbo].[persl_skill](
	[description] [varchar](80) NULL,
	[empid] [int] NOT NULL,
	[skill] [varchar](8) NOT NULL,
	[skill_priority] [int] NOT NULL,
 CONSTRAINT [persl_skill_primary_key] PRIMARY KEY CLUSTERED 
(
	[empid] ASC,
	[skill] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [dbo].[persl_skill] ADD  CONSTRAINT [persl_skill_skill_priority_default]  DEFAULT ((3)) FOR [skill_priority]
GO
ALTER TABLE [dbo].[persl_skill]  WITH CHECK ADD  CONSTRAINT [persl_skill_skill_priority_ck] CHECK  (([skill_priority]>=(1) AND [skill_priority]<=(5)))
GO
ALTER TABLE [dbo].[persl_skill] CHECK CONSTRAINT [persl_skill_skill_priority_ck]
GO
