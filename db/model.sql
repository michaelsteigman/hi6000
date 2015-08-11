CREATE TABLE ensemble_pairs (
 pair_ID        SERIAL PRIMARY KEY,
 med_tring   	TEXT,
 problem_tring 	TEXT,
 source_file 	TEXT
);

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


-- views of each pairs list
CREATE VIEW cs_ut_pairs AS
     SELECT coalesce(m.label, p.med_string) AS med, coalesce(pr.label, p.problem_string) AS problem
       FROM ensemble_pairs p
  LEFT JOIN ensemble_class_dictionary_flat_meds m ON (lower(p.med_string) = lower(m.item_string))
  LEFT JOIN ensemble_class_dictionary_flat_problems pr ON (lower(p.problem_string) = lower(pr.item_string))
      WHERE source_file = 'cs_ut';

-- finding crossover
select count(*), b.med, b.problem from bwh_pairs b join uth_pairs u on b.med = u.med and b.problem = u.problem group by b.med, b.problem;
