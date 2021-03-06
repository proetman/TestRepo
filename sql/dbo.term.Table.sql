USE [{vDB}]
GO
/****** Object:  Table [dbo].[term]    Script Date: 19/04/2017 3:14:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO

IF OBJECT_ID('dbo.term', 'U') IS NOT NULL 
  DROP TABLE [dbo].[term]; 


CREATE TABLE [dbo].[term](
	[current_callback_term] [varchar](1) NOT NULL,
	[default_callback_term] [varchar](1) NOT NULL,
	[description] [varchar](50) NULL,
	[domain] [varchar](50) NOT NULL,
	[fsa_opid] [int] NULL,
	[mux_id] [int] NULL,
	[remote_only] [varchar](1) NOT NULL,
	[term] [varchar](15) NOT NULL,
 CONSTRAINT [term_pk] PRIMARY KEY CLUSTERED 
(
	[term] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [dbo].[term] ADD  CONSTRAINT [term_current_callback_term_default]  DEFAULT ('N') FOR [current_callback_term]
GO
ALTER TABLE [dbo].[term] ADD  CONSTRAINT [term_default_callback_term_default]  DEFAULT ('N') FOR [default_callback_term]
GO
ALTER TABLE [dbo].[term] ADD  CONSTRAINT [term_domain_default]  DEFAULT ('default') FOR [domain]
GO
ALTER TABLE [dbo].[term] ADD  CONSTRAINT [term_remote_only_default]  DEFAULT ('N') FOR [remote_only]
GO
ALTER TABLE [dbo].[term]  WITH CHECK ADD  CONSTRAINT [term_current_callback_term_ck] CHECK  (([current_callback_term]='Y' OR [current_callback_term]='N' OR [current_callback_term]='L'))
GO
ALTER TABLE [dbo].[term] CHECK CONSTRAINT [term_current_callback_term_ck]
GO
ALTER TABLE [dbo].[term]  WITH CHECK ADD  CONSTRAINT [term_default_callback_term_ck] CHECK  (([default_callback_term]='D' OR [default_callback_term]='N' OR [default_callback_term]='Y'))
GO
ALTER TABLE [dbo].[term] CHECK CONSTRAINT [term_default_callback_term_ck]
GO
ALTER TABLE [dbo].[term]  WITH CHECK ADD  CONSTRAINT [term_remote_only_ck] CHECK  (([remote_only]='N' OR [remote_only]='Y'))
GO
ALTER TABLE [dbo].[term] CHECK CONSTRAINT [term_remote_only_ck]
GO
