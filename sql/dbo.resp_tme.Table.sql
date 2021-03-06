USE [{vDB}]
GO
/****** Object:  Table [dbo].[resp_tme]    Script Date: 19/04/2017 3:14:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO

IF OBJECT_ID('dbo.resp_tme', 'U') IS NOT NULL 
  DROP TABLE [dbo].[resp_tme]; 



CREATE TABLE [dbo].[resp_tme](
	[ag_id] [varchar](9) NOT NULL,
	[dgroup] [varchar](5) NOT NULL,
	[dow] [varchar](1) NOT NULL,
	[eta] [int] NOT NULL,
	[hod] [varchar](2) NOT NULL,
 CONSTRAINT [resp_tme_primary_key] PRIMARY KEY CLUSTERED 
(
	[ag_id] ASC,
	[dgroup] ASC,
	[dow] ASC,
	[hod] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [dbo].[resp_tme]  WITH CHECK ADD  CONSTRAINT [resp_tme_dow_ck] CHECK  (([dow]>=(1) AND [dow]<=(7)))
GO
ALTER TABLE [dbo].[resp_tme] CHECK CONSTRAINT [resp_tme_dow_ck]
GO
