BEGIN;

CREATE TABLE IF NOT EXISTS tweets
(
    id serial NOT NULL,
    pub_id bigint NOT NULL,
    pub_author varchar(255) NOT NULL,
    pub_text text NOT NULL,
    pub_img text,
    pub_date time NOT NULL,
    symbol varchar(255),
    sentiment varchar(255),
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS tweets_pub_id_index ON tweets(pub_id);
CREATE INDEX IF NOT EXISTS tweets_symbol_index ON tweets(symbol);

ALTER TABLE public.tweets ADD CONSTRAINT tweets_pub_id_symbol_key UNIQUE (pub_id, symbol);

END;