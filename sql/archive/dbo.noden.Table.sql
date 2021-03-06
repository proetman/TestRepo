USE [AdventureWorks2012]
GO
/****** Object:  Table [dbo].[noden]    Script Date: 19/04/2017 3:14:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[noden](
	[empid] [int] NOT NULL,
	[term] [varchar](15) NOT NULL,
 CONSTRAINT [noden_primary_key] PRIMARY KEY CLUSTERED 
(
	[empid] ASC,
	[term] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
