USE [AdventureWorks2012]
GO
/****** Object:  Table [dbo].[backup_beat_list]    Script Date: 19/04/2017 3:14:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [dbo].[backup_beat_list](
	[backup_beat] [varchar](7) NOT NULL,
	[backup_order] [smallint] NOT NULL,
	[cdts] [varchar](16) NOT NULL,
	[curent] [varchar](1) NOT NULL,
	[delete_id] [varbinary](16) NOT NULL,
	[deplo_plan_name] [varchar](30) NOT NULL,
	[esz_rec_id] [varbinary](16) NOT NULL,
	[min_units] [int] NOT NULL,
	[recovery_unique_id] [varbinary](16) NULL,
	[udts] [varchar](16) NULL,
	[unique_id] [varbinary](16) NOT NULL,
 CONSTRAINT [backup_beat_list_primary_key] PRIMARY KEY NONCLUSTERED 
(
	[unique_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [backup_beat_list_unique2] UNIQUE CLUSTERED 
(
	[esz_rec_id] ASC,
	[backup_order] ASC,
	[delete_id] ASC,
	[recovery_unique_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [backup_beat_list_unique] UNIQUE NONCLUSTERED 
(
	[esz_rec_id] ASC,
	[backup_beat] ASC,
	[delete_id] ASC,
	[recovery_unique_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [dbo].[backup_beat_list] ADD  CONSTRAINT [backup_beat_list_curent_default]  DEFAULT ('T') FOR [curent]
GO
ALTER TABLE [dbo].[backup_beat_list] ADD  CONSTRAINT [backup_beat_list_delete_id_default]  DEFAULT (0x00) FOR [delete_id]
GO
ALTER TABLE [dbo].[backup_beat_list] ADD  CONSTRAINT [backup_beat_list_min_units_default]  DEFAULT ((0)) FOR [min_units]
GO
ALTER TABLE [dbo].[backup_beat_list]  WITH CHECK ADD  CONSTRAINT [backup_beat_list_cdts_ck] CHECK  (([cdts] like '[1-9][0-9][0-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9][0-5][0-9][A-Z][A-Z]' AND CONVERT([int],substring([cdts],(1),(4)))>=(1970) AND ((substring([cdts],(5),(2))='12' OR substring([cdts],(5),(2))='10' OR substring([cdts],(5),(2))='08' OR substring([cdts],(5),(2))='07' OR substring([cdts],(5),(2))='05' OR substring([cdts],(5),(2))='03' OR substring([cdts],(5),(2))='01') AND CONVERT([int],substring([cdts],(7),(2)))<=(31) OR (substring([cdts],(5),(2))='11' OR substring([cdts],(5),(2))='09' OR substring([cdts],(5),(2))='06' OR substring([cdts],(5),(2))='04') AND CONVERT([int],substring([cdts],(7),(2)))<=(30) OR substring([cdts],(5),(2))='02' AND CONVERT([int],substring([cdts],(7),(2)))<=(29)) AND CONVERT([int],substring([cdts],(9),(2)))<=(24) AND substring([cdts],(15),(2))='UT' AND (NOT [cdts] like '[0-9][0-9][0-9][0-9]0229%' OR CONVERT([int],substring([cdts],(1),(4)))%(4)=(0) AND (CONVERT([int],substring([cdts],(1),(4)))%(100)<>(0) OR CONVERT([int],substring([cdts],(1),(4)))%(400)=(0)))))
GO
ALTER TABLE [dbo].[backup_beat_list] CHECK CONSTRAINT [backup_beat_list_cdts_ck]
GO
ALTER TABLE [dbo].[backup_beat_list]  WITH CHECK ADD  CONSTRAINT [backup_beat_list_cur_del_ck] CHECK  (([curent]='T' AND [delete_id]=0x00 OR [curent]='F' AND [delete_id]<>0x00))
GO
ALTER TABLE [dbo].[backup_beat_list] CHECK CONSTRAINT [backup_beat_list_cur_del_ck]
GO
ALTER TABLE [dbo].[backup_beat_list]  WITH CHECK ADD  CONSTRAINT [backup_beat_list_curent_ck] CHECK  (([curent]='F' OR [curent]='T'))
GO
ALTER TABLE [dbo].[backup_beat_list] CHECK CONSTRAINT [backup_beat_list_curent_ck]
GO
ALTER TABLE [dbo].[backup_beat_list]  WITH CHECK ADD  CONSTRAINT [backup_beat_list_min_units_ck] CHECK  (([min_units]>=(0)))
GO
ALTER TABLE [dbo].[backup_beat_list] CHECK CONSTRAINT [backup_beat_list_min_units_ck]
GO
ALTER TABLE [dbo].[backup_beat_list]  WITH CHECK ADD  CONSTRAINT [backup_beat_list_udts_ck] CHECK  (([udts] like '[1-9][0-9][0-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9][0-5][0-9][A-Z][A-Z]' AND CONVERT([int],substring([udts],(1),(4)))>=(1970) AND ((substring([udts],(5),(2))='12' OR substring([udts],(5),(2))='10' OR substring([udts],(5),(2))='08' OR substring([udts],(5),(2))='07' OR substring([udts],(5),(2))='05' OR substring([udts],(5),(2))='03' OR substring([udts],(5),(2))='01') AND CONVERT([int],substring([udts],(7),(2)))<=(31) OR (substring([udts],(5),(2))='11' OR substring([udts],(5),(2))='09' OR substring([udts],(5),(2))='06' OR substring([udts],(5),(2))='04') AND CONVERT([int],substring([udts],(7),(2)))<=(30) OR substring([udts],(5),(2))='02' AND CONVERT([int],substring([udts],(7),(2)))<=(29)) AND CONVERT([int],substring([udts],(9),(2)))<=(24) AND substring([udts],(15),(2))='UT' AND (NOT [udts] like '[0-9][0-9][0-9][0-9]0229%' OR CONVERT([int],substring([udts],(1),(4)))%(4)=(0) AND (CONVERT([int],substring([udts],(1),(4)))%(100)<>(0) OR CONVERT([int],substring([udts],(1),(4)))%(400)=(0)))))
GO
ALTER TABLE [dbo].[backup_beat_list] CHECK CONSTRAINT [backup_beat_list_udts_ck]
GO
