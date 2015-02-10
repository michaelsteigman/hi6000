CREATE TABLE Ensemble_Meds (
 SourceFile  		  TEXT,
 MedString                TEXT,
 UMLSCUI                  TEXT,
 RxCUI                    TEXT,
 RxCUI_Gen		  TEXT,
 Gen			  TEXT,
 NDFRT_MOA		  TEXT,
 NDFRT_MOA_NUI		  TEXT,
 NDFRT_VA_Prod		  TEXT,
 NDFRT_VA_Prod_NUI	  TEXT,
 NDFRT_VA_Class		  TEXT,
 NDFRT_VA_Class_Nui 	  TEXT,
 CONSTRAINT Ensemble_Meds_pk PRIMARY KEY (SourceFile, Medstring, RxCUI)
);

CREATE VIEW BWHMeds AS SELECT * FROM  Ensemble_Meds WHERE SourceFile = 'bwh.txt';
CREATE VIEW BCBSTXMeds AS SELECT * FROM  Ensemble_Meds WHERE SourceFile = 'bcbstx.txt';
CREATE VIEW RepMeds AS SELECT * FROM  Ensemble_Meds WHERE SourceFile = 'rep_ut.txt';
CREATE VIEW CSMeds AS SELECT * FROM  Ensemble_Meds WHERE SourceFile = 'cs_ut.txt';
CREATE VIEW UTHMeds AS SELECT * FROM  Ensemble_Meds WHERE SourceFile = 'uth.txt';
