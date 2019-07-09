CREATE TABLE __h2e_posts
(
    post_id INTEGER PRIMARY KEY
);

INSERT INTO __h2e_posts (post_id) SELECT post_id FROM hive_posts_cache;

CREATE OR REPLACE FUNCTION __fn_h2e_posts()
  RETURNS TRIGGER AS
$func$
BEGIN
    IF NOT EXISTS (SELECT post_id FROM __h2e_posts WHERE post_id = NEW.post_id) THEN
    	INSERT INTO __h2e_posts (post_id) VALUES (NEW.post_id);
	END IF;
	RETURN NEW;
END
$func$ LANGUAGE plpgsql;

CREATE TRIGGER __trg_h2e_posts
AFTER INSERT OR UPDATE ON hive_posts_cache
FOR EACH ROW EXECUTE PROCEDURE __fn_h2e_posts();
