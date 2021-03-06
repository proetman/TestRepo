USE [AdventureWorks2012]
GO
/****** Object:  Table [dbo].[agency_dgroup]    Script Date: 19/04/2017 3:14:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[agency_dgroup](
	[ag_id] [varchar](9) NOT NULL,
	[description] [varchar](50) NULL,
	[dgroup] [varchar](5) NOT NULL,
	[page_id] [varchar](12) NULL,
 CONSTRAINT [agency_dgroup_pk] PRIMARY KEY CLUSTERED 
(
	[ag_id] ASC,
	[dgroup] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
