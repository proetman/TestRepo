USE [{vDB}]
GO
/****** Object:  Table [dbo].[ras_crib_location]    Script Date: 19/04/2017 3:14:56 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO

IF OBJECT_ID('dbo.ras_crib_location', 'U') IS NOT NULL 
  DROP TABLE [dbo].[ras_crib_locatio]; 


CREATE TABLE [dbo].[ras_crib_location](
	[cdts] [varchar](16) NOT NULL,
	[cpers] [int] NOT NULL,
	[crib_location_id] [int] NOT NULL,
	[cterm] [varchar](15) NOT NULL,
	[location] [varchar](255) NOT NULL,
	[type] [varchar](10) NOT NULL,
	[udts] [varchar](16) NULL,
	[unid] [varchar](10) NOT NULL,
	[upers] [int] NULL,
	[uterm] [varchar](15) NULL,
	[x_cord] [int] NOT NULL,
	[y_cord] [int] NOT NULL,
 CONSTRAINT [ras_crib_location_primary_key] PRIMARY KEY CLUSTERED 
(
	[crib_location_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [dbo].[ras_crib_location]  WITH CHECK ADD  CONSTRAINT [ras_crib_location_cdts_ck] CHECK  (([cdts] like '[1-9][0-9][0-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9][0-5][0-9][A-Z][A-Z]' AND CONVERT([int],substring([cdts],(1),(4)))>=(1970) AND ((substring([cdts],(5),(2))='12' OR substring([cdts],(5),(2))='10' OR substring([cdts],(5),(2))='08' OR substring([cdts],(5),(2))='07' OR substring([cdts],(5),(2))='05' OR substring([cdts],(5),(2))='03' OR substring([cdts],(5),(2))='01') AND CONVERT([int],substring([cdts],(7),(2)))<=(31) OR (substring([cdts],(5),(2))='11' OR substring([cdts],(5),(2))='09' OR substring([cdts],(5),(2))='06' OR substring([cdts],(5),(2))='04') AND CONVERT([int],substring([cdts],(7),(2)))<=(30) OR substring([cdts],(5),(2))='02' AND CONVERT([int],substring([cdts],(7),(2)))<=(29)) AND CONVERT([int],substring([cdts],(9),(2)))<=(24) AND substring([cdts],(15),(2))='UT' AND (NOT [cdts] like '[0-9][0-9][0-9][0-9]0229%' OR CONVERT([int],substring([cdts],(1),(4)))%(4)=(0) AND (CONVERT([int],substring([cdts],(1),(4)))%(100)<>(0) OR CONVERT([int],substring([cdts],(1),(4)))%(400)=(0)))))
GO
ALTER TABLE [dbo].[ras_crib_location] CHECK CONSTRAINT [ras_crib_location_cdts_ck]
GO
ALTER TABLE [dbo].[ras_crib_location]  WITH CHECK ADD  CONSTRAINT [ras_crib_location_cpers_ck] CHECK  (([cpers]>=(0)))
GO
ALTER TABLE [dbo].[ras_crib_location] CHECK CONSTRAINT [ras_crib_location_cpers_ck]
GO
ALTER TABLE [dbo].[ras_crib_location]  WITH CHECK ADD  CONSTRAINT [ras_crib_location_udts_ck] CHECK  (([udts] like '[1-9][0-9][0-9][0-9][0-1][0-9][0-3][0-9][0-2][0-9][0-5][0-9][0-5][0-9][A-Z][A-Z]' AND CONVERT([int],substring([udts],(1),(4)))>=(1970) AND ((substring([udts],(5),(2))='12' OR substring([udts],(5),(2))='10' OR substring([udts],(5),(2))='08' OR substring([udts],(5),(2))='07' OR substring([udts],(5),(2))='05' OR substring([udts],(5),(2))='03' OR substring([udts],(5),(2))='01') AND CONVERT([int],substring([udts],(7),(2)))<=(31) OR (substring([udts],(5),(2))='11' OR substring([udts],(5),(2))='09' OR substring([udts],(5),(2))='06' OR substring([udts],(5),(2))='04') AND CONVERT([int],substring([udts],(7),(2)))<=(30) OR substring([udts],(5),(2))='02' AND CONVERT([int],substring([udts],(7),(2)))<=(29)) AND CONVERT([int],substring([udts],(9),(2)))<=(24) AND substring([udts],(15),(2))='UT' AND (NOT [udts] like '[0-9][0-9][0-9][0-9]0229%' OR CONVERT([int],substring([udts],(1),(4)))%(4)=(0) AND (CONVERT([int],substring([udts],(1),(4)))%(100)<>(0) OR CONVERT([int],substring([udts],(1),(4)))%(400)=(0)))))
GO
ALTER TABLE [dbo].[ras_crib_location] CHECK CONSTRAINT [ras_crib_location_udts_ck]
GO
ALTER TABLE [dbo].[ras_crib_location]  WITH CHECK ADD  CONSTRAINT [ras_crib_location_upers_ck] CHECK  (([upers]>=(0)))
GO
ALTER TABLE [dbo].[ras_crib_location] CHECK CONSTRAINT [ras_crib_location_upers_ck]
GO
