CREATE OR REPLACE FUNCTION updated_at() RETURNS trigger
    LANGUAGE plpgsql AS
$$
BEGIN
    NEW.updated_at := current_timestamp;
    RETURN NEW;
END;
$$;;

CREATE TRIGGER updated_at
    BEFORE INSERT OR UPDATE
    ON project.submission
    FOR EACH ROW
EXECUTE FUNCTION updated_at();;

CREATE TRIGGER updated_at
    BEFORE INSERT OR UPDATE
    ON project.author
    FOR EACH ROW
EXECUTE FUNCTION updated_at();;

CREATE TRIGGER updated_at
    BEFORE INSERT OR UPDATE
    ON project.citation
    FOR EACH ROW
EXECUTE FUNCTION updated_at();;

CREATE TRIGGER updated_at
    BEFORE INSERT OR UPDATE
    ON project.summary
    FOR EACH ROW
EXECUTE FUNCTION updated_at();;

CREATE TRIGGER updated_at
    BEFORE INSERT OR UPDATE
    ON project.author_alias
    FOR EACH ROW
EXECUTE FUNCTION updated_at();;

CREATE TRIGGER updated_at
    BEFORE INSERT OR UPDATE
    ON project.author_submission
    FOR EACH ROW
EXECUTE FUNCTION updated_at();;

CREATE TRIGGER updated_at
    BEFORE INSERT OR UPDATE
    ON project.citation_submission
    FOR EACH ROW
EXECUTE FUNCTION updated_at();;
