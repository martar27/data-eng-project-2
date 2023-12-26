CREATE OR REPLACE FUNCTION created_at() RETURNS trigger
    LANGUAGE plpgsql AS
$$
BEGIN
    NEW.created_at := current_timestamp;
    RETURN NEW;
END;
$$;;

CREATE TRIGGER created_at
    BEFORE INSERT
    ON project.submission
    FOR EACH ROW
EXECUTE FUNCTION created_at();;

CREATE TRIGGER created_at
    BEFORE INSERT
    ON project.author
    FOR EACH ROW
EXECUTE FUNCTION created_at();;

CREATE TRIGGER created_at
    BEFORE INSERT
    ON project.citation
    FOR EACH ROW
EXECUTE FUNCTION created_at();;

CREATE TRIGGER created_at
    BEFORE INSERT
    ON project.summary
    FOR EACH ROW
EXECUTE FUNCTION created_at();;

CREATE TRIGGER created_at
    BEFORE INSERT
    ON project.author_alias
    FOR EACH ROW
EXECUTE FUNCTION created_at();;

CREATE TRIGGER created_at
    BEFORE INSERT
    ON project.author_submission
    FOR EACH ROW
EXECUTE FUNCTION created_at();;

CREATE TRIGGER created_at
    BEFORE INSERT
    ON project.citation_submission
    FOR EACH ROW
EXECUTE FUNCTION created_at();;
