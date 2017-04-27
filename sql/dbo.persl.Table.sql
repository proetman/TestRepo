USE [{vDB}]
GO
/****** Object:  Table [dbo].[persl]    Script Date: 19/04/2017 3:14:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO

IF OBJECT_ID('dbo.persl', 'U') IS NOT NULL 
  DROP TABLE [dbo].[persl]; 


CREATE TABLE [dbo].[persl](
	[ag_id] [varchar](9) NOT NULL,
	[app_type] [varchar](30) NULL,
	[bl_typ] [varchar](5) NULL,
	[cad_cmd_mask] [int] NOT NULL,
	[chatid] [int] NULL,
	[curent] [varchar](1) NOT NULL,
	[dbm_cmd_mask] [int] NOT NULL,
	[default_unid] [varchar](10) NULL,
	[delex] [int] NOT NULL,
	[delno] [int] NOT NULL,
	[domain] [varchar](64) NULL,
	[email] [varchar](80) NULL,
	[emp_add] [varchar](30) NULL,
	[emp_city] [varchar](20) NULL,
	[emp_num] [varchar](16) NULL,
	[emp_st] [varchar](4) NULL,
	[empid] [int] NOT NULL,
	[fname] [varchar](50) NULL,
	[host_term] [varchar](15) NULL,
	[ldts] [varchar](16) NULL,
	[lname] [varchar](50) NULL,
	[lodts] [varchar](16) NULL,
	[logged_on] [varchar](1) NOT NULL,
	[mi] [varchar](1) NULL,
	[not_add] [varchar](30) NULL,
	[not_city] [varchar](20) NULL,
	[not_nme] [varchar](25) NULL,
	[not_ph] [varchar](14) NULL,
	[not_st] [varchar](4) NULL,
	[page_id] [varchar](12) NULL,
	[pass_date] [bigint] NOT NULL,
	[pcust1] [varchar](50) NULL,
	[pcust2] [varchar](50) NULL,
	[pcust3] [varchar](50) NULL,
	[pcust4] [varchar](50) NULL,
	[phone] [varchar](14) NULL,
	[pid] [int] NULL,
	[pswrd] [varchar](128) NULL,
	[pswrd_hash_id] [int] NULL,
	[ras_club_code] [varchar](10) NULL,
	[rms_id] [int] NULL,
	[soundex] [varchar](4) NULL,
	[term] [varchar](15) NULL,
	[usr_id] [varchar](20) NOT NULL,
 CONSTRAINT [persl_primary_key] PRIMARY KEY CLUSTERED 
(
	[empid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [persl_unique] UNIQUE NONCLUSTERED 
(
	[usr_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [dbo].[persl] ADD  CONSTRAINT [persl_cad_cmd_mask_default]  DEFAULT ((0)) FOR [cad_cmd_mask]
GO
ALTER TABLE [dbo].[persl] ADD  CONSTRAINT [persl_curent_default]  DEFAULT ('T') FOR [curent]
GO
ALTER TABLE [dbo].[persl] ADD  CONSTRAINT [persl_dbm_cmd_mask_default]  DEFAULT ((0)) FOR [dbm_cmd_mask]
GO
ALTER TABLE [dbo].[persl] ADD  CONSTRAINT [persl_logged_on_default]  DEFAULT ('F') FOR [logged_on]
GO
ALTER TABLE [dbo].[persl]  WITH CHECK ADD  CONSTRAINT [persl_curent_ck] CHECK  (([curent]='F' OR [curent]='T'))
GO
ALTER TABLE [dbo].[persl] CHECK CONSTRAINT [persl_curent_ck]
GO
ALTER TABLE [dbo].[persl]  WITH CHECK ADD  CONSTRAINT [persl_delex_ck] CHECK  (([Delex]>=(0) AND [Delex]<=(9999)))
GO
ALTER TABLE [dbo].[persl] CHECK CONSTRAINT [persl_delex_ck]
GO
ALTER TABLE [dbo].[persl]  WITH CHECK ADD  CONSTRAINT [persl_delex_gt_delno_ck] CHECK  (([Delex]>[delno]))
GO
ALTER TABLE [dbo].[persl] CHECK CONSTRAINT [persl_delex_gt_delno_ck]
GO
ALTER TABLE [dbo].[persl]  WITH CHECK ADD  CONSTRAINT [persl_delno_ck] CHECK  (([Delno]>=(0) AND [Delno]<=(9998)))
GO
ALTER TABLE [dbo].[persl] CHECK CONSTRAINT [persl_delno_ck]
GO
ALTER TABLE [dbo].[persl]  WITH CHECK ADD  CONSTRAINT [persl_empid_ck] CHECK  (([empid]>=(0)))
GO
ALTER TABLE [dbo].[persl] CHECK CONSTRAINT [persl_empid_ck]
GO
ALTER TABLE [dbo].[persl]  WITH CHECK ADD  CONSTRAINT [persl_ldts_ck] CHECK  (([ldts] like '[1-9][0-9][0-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9][0-5][0-9][A-Z][A-Z]' AND CONVERT([int],substring([ldts],(1),(4)))>=(1970) AND ((substring([ldts],(5),(2))='12' OR substring([ldts],(5),(2))='10' OR substring([ldts],(5),(2))='08' OR substring([ldts],(5),(2))='07' OR substring([ldts],(5),(2))='05' OR substring([ldts],(5),(2))='03' OR substring([ldts],(5),(2))='01') AND CONVERT([int],substring([ldts],(7),(2)))<=(31) OR (substring([ldts],(5),(2))='11' OR substring([ldts],(5),(2))='09' OR substring([ldts],(5),(2))='06' OR substring([ldts],(5),(2))='04') AND CONVERT([int],substring([ldts],(7),(2)))<=(30) OR substring([ldts],(5),(2))='02' AND CONVERT([int],substring([ldts],(7),(2)))<=(29)) AND CONVERT([int],substring([ldts],(9),(2)))<=(24) AND substring([ldts],(15),(2))='UT' AND (NOT [ldts] like '[0-9][0-9][0-9][0-9]0229%' OR CONVERT([int],substring([ldts],(1),(4)))%(4)=(0) AND (CONVERT([int],substring([ldts],(1),(4)))%(100)<>(0) OR CONVERT([int],substring([ldts],(1),(4)))%(400)=(0)))))
GO
ALTER TABLE [dbo].[persl] CHECK CONSTRAINT [persl_ldts_ck]
GO
ALTER TABLE [dbo].[persl]  WITH CHECK ADD  CONSTRAINT [persl_lodts_ck] CHECK  (([lodts] like '[1-9][0-9][0-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9][0-5][0-9][A-Z][A-Z]' AND CONVERT([int],substring([lodts],(1),(4)))>=(1970) AND ((substring([lodts],(5),(2))='12' OR substring([lodts],(5),(2))='10' OR substring([lodts],(5),(2))='08' OR substring([lodts],(5),(2))='07' OR substring([lodts],(5),(2))='05' OR substring([lodts],(5),(2))='03' OR substring([lodts],(5),(2))='01') AND CONVERT([int],substring([lodts],(7),(2)))<=(31) OR (substring([lodts],(5),(2))='11' OR substring([lodts],(5),(2))='09' OR substring([lodts],(5),(2))='06' OR substring([lodts],(5),(2))='04') AND CONVERT([int],substring([lodts],(7),(2)))<=(30) OR substring([lodts],(5),(2))='02' AND CONVERT([int],substring([lodts],(7),(2)))<=(29)) AND CONVERT([int],substring([lodts],(9),(2)))<=(24) AND substring([lodts],(15),(2))='UT' AND (NOT [lodts] like '[0-9][0-9][0-9][0-9]0229%' OR CONVERT([int],substring([lodts],(1),(4)))%(4)=(0) AND (CONVERT([int],substring([lodts],(1),(4)))%(100)<>(0) OR CONVERT([int],substring([lodts],(1),(4)))%(400)=(0)))))
GO
ALTER TABLE [dbo].[persl] CHECK CONSTRAINT [persl_lodts_ck]
GO
ALTER TABLE [dbo].[persl]  WITH CHECK ADD  CONSTRAINT [persl_logged_on_ck] CHECK  (([logged_on]='F' OR [logged_on]='T'))
GO
ALTER TABLE [dbo].[persl] CHECK CONSTRAINT [persl_logged_on_ck]
GO
ALTER TABLE [dbo].[persl]  WITH CHECK ADD  CONSTRAINT [persl_pswrd_hash_id_ck] CHECK  (([pswrd_hash_id]=(2) OR [pswrd_hash_id]=(1) OR [pswrd_hash_id]=(0)))
GO
ALTER TABLE [dbo].[persl] CHECK CONSTRAINT [persl_pswrd_hash_id_ck]
GO
ALTER TABLE [dbo].[persl]  WITH CHECK ADD  CONSTRAINT [persl_pswrd_hash_null_ck] CHECK  (([pswrd] IS NULL AND [pswrd_hash_id] IS NULL) OR ([pswrd] IS NOT NULL AND [pswrd_hash_id] IS NOT NULL))
GO
ALTER TABLE [dbo].[persl] CHECK CONSTRAINT [persl_pswrd_hash_null_ck]
GO
