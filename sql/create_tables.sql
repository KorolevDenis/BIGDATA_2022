drop table journal CASCADE;
drop table publication_author CASCADE;
drop table publication CASCADE;
drop table name_variant CASCADE;
drop table author CASCADE;
drop table affiliation CASCADE;
drop table publication_subject CASCADE;
drop table subject CASCADE;
drop table freetoread CASCADE;
drop table freetoread_label CASCADE;
drop table publication_freetoread CASCADE;
drop table publication_freetoread_label CASCADE;
drop table current_affiliation CASCADE;
drop table publication_citations CASCADE;
drop table journal_subject CASCADE;

CREATE TABLE affiliation (
	id bigserial CONSTRAINT affiliation_pkey PRIMARY KEY,
	afid bigint,
	name varchar,
	city varchar,
	country varchar
);

CREATE TABLE name_variant (
	id bigint CONSTRAINT name_variant_pkey PRIMARY KEY,
	name varchar,
	city varchar,
	country varchar
);

--create unique index affiliation_un_ind on affiliation (afid, name);

CREATE TABLE author (
	id bigserial CONSTRAINT author_pkey PRIMARY KEY,
	auid bigint,
	name varchar
);

CREATE TABLE subject (
	id serial CONSTRAINT subject_pkey PRIMARY KEY,
	short_name varchar,
	name varchar
);

insert into subject (short_name, name) values ('AGRI', 'Agricultural and Biological Sciences');
insert into subject (short_name,name) values ('ARTS', 'Arts and Humanities');
insert into subject (short_name,name) values ('BIOC', 'Biochemistry, Genetics and Molecular Biology');
insert into subject (short_name,name) values ('BUSI', 'Business, Management and Accounting');
insert into subject (short_name,name) values ('CENG', 'Chemical Engineering');
insert into subject (short_name,name) values ('CHEM', 'Chemistry');
insert into subject (short_name,name) values ('COMP', 'Computer Science');
insert into subject (short_name,name) values ('DECI', 'Decision Sciences');
insert into subject (short_name,name) values ('DENT', 'Dentistry');
insert into subject (short_name,name) values ('EART', 'Earth and Planetary Sciences');
insert into subject (short_name,name) values ('ECON', 'Economics, Econometrics and Finance');
insert into subject (short_name,name) values ('ENER', 'Energy');
insert into subject (short_name,name) values ('ENGI', 'Engineering');
insert into subject (short_name,name) values ('ENVI', 'Environmental Science');
insert into subject (short_name,name) values ('HEAL', 'Health Professions');
insert into subject (short_name,name) values ('IMMU', 'Immunology and Microbiology');
insert into subject (short_name,name) values ('MATE', 'Materials Science');
insert into subject (short_name,name) values ('MATH', 'Mathematics');
insert into subject (short_name,name) values ('MEDI', 'Medicine');
insert into subject (short_name,name) values ('NEUR', 'Neuroscience');
insert into subject (short_name,name) values ('NURS', 'Nursing');
insert into subject (short_name,name) values ('PHAR', 'Pharmacology, Toxicology and Pharmaceutics');
insert into subject (short_name,name) values ('PHYS', 'Physics and Astronomy');
insert into subject (short_name,name) values ('PSYC', 'Psychology');
insert into subject (short_name,name) values ('SOCI', 'Social Sciences');
insert into subject (short_name,name) values ('VETE', 'Veterinary');
insert into subject (short_name,name) values ('MULT', 'Multidisciplinary');
insert into subject (short_name,name) values ('Undefined', '');


CREATE TABLE journal (
	id bigserial CONSTRAINT journal_pkey PRIMARY KEY,
	source_id bigint,
    name varchar,
    issn varchar,
    e_issn varchar,
    aggregation_type varchar,
    cite_score real,
    sjr real,
    snip real,
    year_of_scores varchar(4),
    publisher varchar
);

create unique index journal_un_ind on journal (issn, e_issn, name);

CREATE TABLE publication (
	id bigserial, --CONSTRAINT publication_pkey PRIMARY KEY,
	
	link_scopus varchar,
    link_citedby varchar,
	
	dc_identifier varchar,
	eid varchar,
	title varchar,
	volume varchar,
	issue_identifier varchar,
	page_range varchar,
	cover_date varchar,
	doi varchar,
	citedby_count integer,
	
	pubmed_id varchar,
	journal_id integer,
subtype varchar,
	
	article_number varchar,
	source_id varchar,
	openaccess boolean,
	
	keywords varchar,
	abstract varchar,
	raw_xml varchar,
	cur_time timestamp,
	api_key varchar ,
	is_scopus boolean, 
	log_date integer
) PARTITION BY LIST (log_date);

alter table publication ADD CONSTRAINT publication_pkey PRIMARY KEY (id, log_date);
alter table publication ADD CONSTRAINT publication_fkey FOREIGN KEY(journal_id) REFERENCES journal(id) ON UPDATE CASCADE ON DELETE CASCADE;
	  
--create unique index publication_log_date_ind on publication (dc_identifier, log_date);
--create unique index publication_id_ind on publication (dc_identifier);

CREATE TABLE freetoread (
	id serial CONSTRAINT freetoread_pkey PRIMARY KEY,
name varchar
);

CREATE TABLE freetoread_label (
	id serial CONSTRAINT freetoread_label_pkey PRIMARY KEY,
name varchar
);

CREATE TABLE publication_freetoread (
log_date integer,
publication_id integer,
freetoread_id integer
) PARTITION BY LIST (log_date);

alter table publication_freetoread ADD CONSTRAINT publication_freetoread_pkey PRIMARY KEY (log_date, publication_id, freetoread_id);
alter table publication_freetoread ADD CONSTRAINT publication_freetoread_fkey FOREIGN KEY(publication_id, log_date) REFERENCES publication(id, log_date) ON UPDATE CASCADE ON DELETE CASCADE;
alter table publication_freetoread ADD CONSTRAINT publication_freetoread_fkey2 FOREIGN KEY(freetoread_id) REFERENCES freetoread(id) ON UPDATE CASCADE ON DELETE CASCADE;

CREATE TABLE publication_freetoread_label (
log_date integer,
publication_id integer,
freetoread_label_id integer
) PARTITION BY LIST (log_date);

alter table publication_freetoread_label ADD CONSTRAINT publication_freetoread_label_pkey PRIMARY KEY (log_date, publication_id, freetoread_label_id);
alter table publication_freetoread_label ADD CONSTRAINT publication_freetoread_label_fkey FOREIGN KEY(publication_id, log_date) REFERENCES publication(id, log_date) ON UPDATE CASCADE ON DELETE CASCADE;
alter table publication_freetoread_label ADD CONSTRAINT publication_freetoread_label_fkey2 FOREIGN KEY(freetoread_label_id) REFERENCES freetoread_label(id) ON UPDATE CASCADE ON DELETE CASCADE;

insert into freetoread (name) values ('all');
insert into freetoread (name) values ('publisherhybridgold');
insert into freetoread (name) values ('repository');
insert into freetoread (name) values ('repositoryvor');
insert into freetoread (name) values ('repositoryam');
insert into freetoread (name) values ('publisherfree2read');

insert into freetoread_label (name) values ('All Open Access');
insert into freetoread_label (name) values ('Green');
insert into freetoread_label (name) values ('Hybrid Gold');


CREATE TABLE publication_author (
log_date integer,
publication_id integer,
author_id integer,
affiliation_id  integer
) PARTITION BY LIST (log_date);

alter table publication_author ADD CONSTRAINT publication_author_pkey PRIMARY KEY (log_date, publication_id, author_id, affiliation_id);
alter table publication_author ADD CONSTRAINT publication_author_fkey FOREIGN KEY(publication_id, log_date) REFERENCES publication(id, log_date) ON UPDATE CASCADE ON DELETE CASCADE;
alter table publication_author ADD CONSTRAINT publication_author_fkey2 FOREIGN KEY(author_id) REFERENCES author(id) ON UPDATE CASCADE ON DELETE CASCADE;
alter table publication_author ADD CONSTRAINT publication_author_fkey3 FOREIGN KEY(affiliation_id) REFERENCES affiliation(id) ON UPDATE CASCADE ON DELETE CASCADE;


CREATE TABLE publication_subject (
log_date integer,
publication_id integer,
subject_id integer
) PARTITION BY LIST (log_date);

alter table publication_subject ADD CONSTRAINT publication_subject_pkey PRIMARY KEY (log_date, publication_id, subject_id);
alter table publication_subject ADD CONSTRAINT publication_subject_fkey FOREIGN KEY(publication_id, log_date) REFERENCES publication(id, log_date) ON UPDATE CASCADE ON DELETE CASCADE;
--alter table publication_subject ADD CONSTRAINT publication_subject_fkey2 FOREIGN KEY(subject_id) REFERENCES subject(id) ON UPDATE CASCADE ON DELETE CASCADE;


CREATE TABLE journal_subject (
journal_id integer  REFERENCES journal(id) ON UPDATE CASCADE ON DELETE CASCADE,
subject_id integer  REFERENCES subject(id) ON UPDATE CASCADE ON DELETE CASCADE
);


CREATE TABLE publication_citations (
log_date integer,
publication_id integer,

log_citation_date integer,
publication_citation_id integer
);

alter table journal_subject ADD CONSTRAINT journal_subject_pkey PRIMARY KEY (journal_id, subject_id);
alter table publication_citations ADD CONSTRAINT publication_citations_pkey PRIMARY KEY (log_date, log_citation_date, publication_id, publication_citation_id);
alter table publication_citations ADD CONSTRAINT publication_citations_fkey FOREIGN KEY(publication_id, log_date) REFERENCES publication(id, log_date) ON UPDATE CASCADE ON DELETE CASCADE;
alter table publication_citations ADD CONSTRAINT publication_subject_fkey2 FOREIGN KEY(publication_citation_id, log_citation_date) REFERENCES publication(id, log_date) ON UPDATE CASCADE ON DELETE CASCADE;


--select * from publication t1 
--where exists 
 --     (select 1 from publication t2 
 --      where t1.title = t2.title and t1.eid <> t2.eid) 
-- ;
 
--select * from publication JOIN journal on journal.id=journal_id where publication.id=244;
--select * from publication JOIN current_affiliation on publication.id=publication_id JOIN affiliation on affiliation.id=affiliation_id where publication.id=244;
--select * from publication JOIN publication_author on publication.id=publication_id JOIN author on author.id=author_id where publication.id=244;