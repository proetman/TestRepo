USE [Upload_Test]
GO
/****** Object:  Table [dbo].[beat]    Script Date: 19/04/2017 3:14:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[beat](
	[cdts] [varchar](16) NOT NULL,
	[comm] [varchar](80) NULL,
	[curent] [varchar](1) NOT NULL,
	[delete_id] [varbinary](16) NOT NULL,
	[deplo_plan_name] [varchar](30) NOT NULL,
	[lev3] [varchar](7) NOT NULL,
	[lev4] [varchar](5) NULL,
	[lev5] [varchar](5) NULL,
	[map_no] [varchar](8) NULL,
	[mun] [varchar](40) NULL,
	[recovery_unique_id] [varbinary](16) NULL,
	[udts] [varchar](16) NULL,
	[unique_id] [varbinary](16) NOT NULL,
 CONSTRAINT [beat_primary_key] PRIMARY KEY NONCLUSTERED 
(
	[unique_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [beat_unique] UNIQUE CLUSTERED 
(
	[deplo_plan_name] ASC,
	[lev3] ASC,
	[delete_id] ASC,
	[recovery_unique_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [dbo].[beat] ADD  CONSTRAINT [beat_curent_default]  DEFAULT ('T') FOR [curent]
GO
ALTER TABLE [dbo].[beat] ADD  CONSTRAINT [beat_delete_id_default]  DEFAULT (0x00) FOR [delete_id]
GO
ALTER TABLE [dbo].[beat]  WITH CHECK ADD  CONSTRAINT [beat_cdts_ck] CHECK  (([cdts] like '[1-9][0-9][0-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9][0-5][0-9][A-Z][A-Z]' AND CONVERT([int],substring([cdts],(1),(4)))>=(1970) AND ((substring([cdts],(5),(2))='12' OR substring([cdts],(5),(2))='10' OR substring([cdts],(5),(2))='08' OR substring([cdts],(5),(2))='07' OR substring([cdts],(5),(2))='05' OR substring([cdts],(5),(2))='03' OR substring([cdts],(5),(2))='01') AND CONVERT([int],substring([cdts],(7),(2)))<=(31) OR (substring([cdts],(5),(2))='11' OR substring([cdts],(5),(2))='09' OR substring([cdts],(5),(2))='06' OR substring([cdts],(5),(2))='04') AND CONVERT([int],substring([cdts],(7),(2)))<=(30) OR substring([cdts],(5),(2))='02' AND CONVERT([int],substring([cdts],(7),(2)))<=(29)) AND CONVERT([int],substring([cdts],(9),(2)))<=(24) AND substring([cdts],(15),(2))='UT' AND (NOT [cdts] like '[0-9][0-9][0-9][0-9]0229%' OR CONVERT([int],substring([cdts],(1),(4)))%(4)=(0) AND (CONVERT([int],substring([cdts],(1),(4)))%(100)<>(0) OR CONVERT([int],substring([cdts],(1),(4)))%(400)=(0)))))
GO
ALTER TABLE [dbo].[beat] CHECK CONSTRAINT [beat_cdts_ck]
GO
ALTER TABLE [dbo].[beat]  WITH CHECK ADD  CONSTRAINT [beat_cur_del_ck] CHECK  (([curent]='T' AND [delete_id]=0x00 OR [curent]='F' AND [delete_id]<>0x00))
GO
ALTER TABLE [dbo].[beat] CHECK CONSTRAINT [beat_cur_del_ck]
GO
ALTER TABLE [dbo].[beat]  WITH CHECK ADD  CONSTRAINT [beat_curent_ck] CHECK  (([curent]='F' OR [curent]='T'))
GO
ALTER TABLE [dbo].[beat] CHECK CONSTRAINT [beat_curent_ck]
GO
ALTER TABLE [dbo].[beat]  WITH CHECK ADD  CONSTRAINT [beat_udts_ck] CHECK  (([udts] like '[1-9][0-9][0-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9][0-5][0-9][A-Z][A-Z]' AND CONVERT([int],substring([udts],(1),(4)))>=(1970) AND ((substring([udts],(5),(2))='12' OR substring([udts],(5),(2))='10' OR substring([udts],(5),(2))='08' OR substring([udts],(5),(2))='07' OR substring([udts],(5),(2))='05' OR substring([udts],(5),(2))='03' OR substring([udts],(5),(2))='01') AND CONVERT([int],substring([udts],(7),(2)))<=(31) OR (substring([udts],(5),(2))='11' OR substring([udts],(5),(2))='09' OR substring([udts],(5),(2))='06' OR substring([udts],(5),(2))='04') AND CONVERT([int],substring([udts],(7),(2)))<=(30) OR substring([udts],(5),(2))='02' AND CONVERT([int],substring([udts],(7),(2)))<=(29)) AND CONVERT([int],substring([udts],(9),(2)))<=(24) AND substring([udts],(15),(2))='UT' AND (NOT [udts] like '[0-9][0-9][0-9][0-9]0229%' OR CONVERT([int],substring([udts],(1),(4)))%(4)=(0) AND (CONVERT([int],substring([udts],(1),(4)))%(100)<>(0) OR CONVERT([int],substring([udts],(1),(4)))%(400)=(0)))))
GO
ALTER TABLE [dbo].[beat] CHECK CONSTRAINT [beat_udts_ck]
GO
