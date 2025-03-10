BEGIN;

CREATE TABLE IF NOT EXISTS tweets
(
    id serial NOT NULL,
    pub_id bigint NOT NULL UNIQUE,
    pub_author varchar(255) NOT NULL,
    pub_text text NOT NULL,
    pub_img text,
    pub_date time NOT NULL,
    PRIMARY KEY (id)
);


END;