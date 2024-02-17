CREATE TABLE IF NOT EXISTS detailed_report
(
    detailed_report_id SERIAL PRIMARY KEY,
    payment_id INT NOT NULL,
    customer_id SMALLINT  NOT NULL,
    staff_id SMALLINT NOT NULL,
    rental_id INT NOT NULL,
    amount NUMERIC(5,2) NOT NULL,
    payment_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    first_name VARCHAR(45),
    last_name VARCHAR(45),
    rental_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    return_date TIMESTAMP WITHOUT TIME ZONE,
    FOREIGN KEY (staff_id) REFERENCES staff(staff_id),
    FOREIGN KEY (payment_id) REFERENCES payment(payment_id),
    FOREIGN KEY (rental_id) REFERENCES rental(rental_id)
);

INSERT INTO detailed_report (payment_id, customer_id, staff_id, rental_id, amount, payment_date, first_name, last_name, rental_date, return_date)
SELECT
    p.payment_id, 
    r.customer_id, 
    s.staff_id, 
    r.rental_id, 
    p.amount, 
    p.payment_date, 
    s.first_name, 
    s.last_name, 
    r.rental_date, 
    r.return_date
FROM
	payment p
JOIN 
	rental r ON r.rental_id = p.rental_id
JOIN
	staff s ON s.staff_id = p.staff_id;

SELECT * FROM detailed_report;

CREATE TABLE IF NOT EXISTS summary_report
(
    staff_id SMALLINT NOT NULL,
    first_name VARCHAR(45) NOT NULL,
    last_name VARCHAR(45) NOT NULL,
    total_sales NUMERIC(10,2) NOT NULL,
    report_month DATE NOT NULL,
    CONSTRAINT summary_report_pkey PRIMARY KEY (staff_id, report_month)
);

CREATE OR REPLACE TRIGGER detailed_report_after_change
AFTER INSERT OR UPDATE OR DELETE ON detailed_report
FOR EACH ROW EXECUTE FUNCTION update_summary_report();

CREATE OR REPLACE FUNCTION update_summary_report()
RETURNS TRIGGER AS
$$
BEGIN
    -- Determine the month of the affected row
    -- Use NEW for insert/update operations and OLD for delete
    -- COALESCE is used to handle both insert/delete cases
    DECLARE
        affected_month DATE;
    BEGIN
        affected_month := DATE_TRUNC('month', COALESCE(NEW.return_date, OLD.return_date));

        -- Delete the existing summary for the affected month
        DELETE FROM summary_report
        WHERE staff_id = COALESCE(NEW.staff_id, OLD.staff_id)
        AND report_month = affected_month;

        -- Recalculate and insert the new summary for the affected month
        INSERT INTO summary_report (staff_id, first_name, last_name, total_sales, report_month)
        SELECT
            staff_id,
            first_name,
            last_name,
            SUM(amount) AS total_sales,
            affected_month AS report_month
        FROM
            detailed_report
        WHERE
            DATE_TRUNC('month', return_date) = affected_month
        AND
            staff_id = COALESCE(NEW.staff_id, OLD.staff_id)
        GROUP BY
            staff_id, first_name, last_name;

        RETURN NULL; -- Since this is an AFTER trigger
    END;
END;
$$

CREATE OR REPLACE PROCEDURE refresh_reports_and_find_salespersons()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Drop the tables if they exist
    DROP TABLE IF EXISTS detailed_report, summary_report;

    -- Recreate the detailed_report table
    CREATE TABLE detailed_report (
        detailed_report_id SERIAL PRIMARY KEY,
        payment_id INTEGER NOT NULL,
        customer_id SMALLINT NOT NULL,
        staff_id SMALLINT NOT NULL,
        rental_id INTEGER NOT NULL,
        amount NUMERIC(5,2) NOT NULL,
        payment_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        first_name VARCHAR(45),
        last_name VARCHAR(45),
        rental_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        return_date TIMESTAMP WITHOUT TIME ZONE
    );

    -- Recreate the summary_report table
    CREATE TABLE summary_report (
        staff_id SMALLINT NOT NULL,
        first_name VARCHAR(45) NOT NULL,
        last_name VARCHAR(45) NOT NULL,
        total_sales NUMERIC(10,2) NOT NULL,
        report_month DATE NOT NULL,
        PRIMARY KEY (staff_id, report_month)
    );

    INSERT INTO detailed_report (payment_id, customer_id, staff_id, rental_id, amount, payment_date, first_name, last_name, rental_date, return_date)
SELECT
    p.payment_id, 
    r.customer_id, 
    s.staff_id, 
    r.rental_id, 
    p.amount, 
    p.payment_date, 
    s.first_name, 
    s.last_name, 
    r.rental_date, 
    r.return_date
FROM
	payment p
JOIN 
	rental r ON r.rental_id = p.rental_id
JOIN
	staff s ON s.staff_id = p.staff_id;

    -- Call this function for the most recent complete month
    PERFORM generate_summary_report((DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month')::DATE);

END;
$$;