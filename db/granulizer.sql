create table ensemble_class_dictionary (
 item_string         TEXT,
 concept	     TEXT,
 label		     TEXT,
 vocabs 	     TEXT[],
 semantic_types      TEXT[],
 status		     TEXT,
 version 	     TEXT,
 PRIMARY KEY (item_string, concept)
);

-- flatten multiple concepts, concatenating the label
CREATE VIEW ensemble_class_dictionary_flat_problems AS
     SELECT item_string, string_agg(label, ', ') AS label
       FROM ensemble_class_dictionary
   GROUP BY item_string;

-- flatten dupe meds caused by varying case
--
-- this is a quick fix - if we do metamap for meds, should
-- give meds the cat * | lower | sort | uniq treatment
CREATE VIEW ensemble_class_dictionary_flat_meds AS
     SELECT upper(item_string) AS item_string, label
       FROM ensemble_class_dictionary
   GROUP BY upper(item_string), label;

