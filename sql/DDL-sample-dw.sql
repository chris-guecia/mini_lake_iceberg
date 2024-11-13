
-- Create dim_product table
DROP TABLE IF EXISTS nessie.warehouse.dim_product;
CREATE TABLE nessie.warehouse.dim_product (
    product_sk BIGINT,
    product_id VARCHAR,
    product_name VARCHAR,
    subject_area VARCHAR,
    grade_range VARCHAR,
    is_active BOOLEAN,
    created_at TIMESTAMP
);
-- Insert sample data into dim_product
INSERT INTO nessie.warehouse.dim_product VALUES
(1, 'SCIENCE_01', 'science', 'Science', '9-12', true, '2023-01-01 00:00:00'),
(2, 'MATH_01', 'math', 'Mathematics', '9-12', true, '2023-01-01 00:00:00'),
(3, 'ELA_01', 'ela', 'English Language Arts', '9-12', true, '2023-01-01 00:00:00');


-- Create dim_object table
DROP TABLE IF EXISTS nessie.warehouse.dim_object;
CREATE TABLE nessie.warehouse.dim_object (
    object_sk BIGINT,
    object_type VARCHAR,
    object_category VARCHAR,
    is_interactive BOOLEAN
);
-- Insert sample data into dim_object
INSERT INTO nessie.warehouse.dim_object VALUES
(1, 'button', 'Navigation', true),
(2, 'video', 'Content', true),
(3, 'quiz', 'Assessment', true),
(4, 'assignment', 'Assessment', true),
(5, 'homework', 'Assessment', true),
(6, 'lesson', 'Content', true),
(7, 'worksheet', 'Content', false),
(8, 'project', 'Assessment', true),
(9, 'discussion', 'Collaboration', true),
(10, 'resource', 'Content', false);

-- Create dim_verb table
DROP TABLE IF EXISTS nessie.warehouse.dim_verb;
CREATE TABLE nessie.warehouse.dim_verb (
    verb_sk BIGINT,
    verb VARCHAR,
    verb_category VARCHAR,
    is_active BOOLEAN
);
-- Insert sample data into dim_verb
INSERT INTO nessie.warehouse.dim_verb VALUES
(1, 'selected', 'Navigation', true),
(2, 'viewed', 'Content Access', true),
(3, 'completed', 'Assessment', true),
(4, 'started', 'Progress', true),
(5, 'submitted', 'Assessment', true),
(6, 'downloaded', 'Content Access', true),
(7, 'printed', 'Content Access', true),
(8, 'shared', 'Collaboration', true),
(9, 'commented', 'Collaboration', true),
(10, 'rated', 'Engagement', true),
(11, 'bookmarked', 'Engagement', true);


-- Create fact_events table
CREATE TABLE nessie.warehouse.fact_events (
    event_id VARCHAR,
    user_sk BIGINT,
    product_sk BIGINT,
    object_sk BIGINT,
    verb_sk BIGINT,
    event_timestamp TIMESTAMP,
    event_date DATE
);

-- Create dim_user_type2 table with SCD Type 2 attributes
DROP TABLE IF EXISTS nessie.warehouse.dim_user_type2;
CREATE TABLE nessie.warehouse.dim_user_type2 (
    user_sk BIGINT,  -- Surrogate key
    user_id VARCHAR, -- Natural key
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR,
    user_type VARCHAR,
    grade_level VARCHAR,
    school_id VARCHAR,
    effective_date TIMESTAMP,
    expiration_date TIMESTAMP,
    is_current BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);


-- Insert sample data into dim_user_type2with SCD Type 2
INSERT INTO nessie.warehouse.dim_user_type2
VALUES
(1, '616700523571', 'John', 'Doe', 'john.doe@school.edu', 'student', '9', 'SCH001',
 '2023-01-01 00:00:00', '9999-12-31 23:59:59', true, '2023-01-01 00:00:00', '2023-01-01 00:00:00'),
(2, '616700523341', 'Jane', 'Smith', 'jane.smith@school.edu', 'student', '10', 'SCH001',
 '2023-01-01 00:00:00', '9999-12-31 23:59:59', true, '2023-01-01 00:00:00', '2023-01-01 00:00:00'),
(3, '716800634682', 'Mike', 'Johnson', 'mike.j@school.edu', 'student', '8', 'SCH002',
 '2023-01-01 00:00:00', '9999-12-31 23:59:59', true, '2023-01-01 00:00:00', '2023-01-01 00:00:00'),
(4, '816900745793', 'Sarah', 'Wilson', 'sarah.w@school.edu', 'student', '11', 'SCH002',
 '2023-01-01 00:00:00', '9999-12-31 23:59:59', true, '2023-01-01 00:00:00', '2023-01-01 00:00:00'),
(5, '916000856804', 'Robert', 'Brown', 'robert.b@school.edu', 'student', '12', 'SCH003',
 '2023-01-01 00:00:00', '9999-12-31 23:59:59', true, '2023-01-01 00:00:00', '2023-01-01 00:00:00');


-- Example of SCD Type 2 update (when a student changes grade level)
INSERT INTO nessie.warehouse.dim_user_type2
VALUES
(6, '616700523571', 'John', 'Doe', 'john.doe@school.edu', 'student', '10', 'SCH001',
 '2023-09-01 00:00:00', '9999-12-31 23:59:59', true, '2023-09-01 00:00:00', '2023-09-01 00:00:00');

UPDATE nessie.warehouse.dim_user_type2
SET expiration_date = '2023-09-01 00:00:00',
    is_current = false
WHERE user_id = '616700523571'
  AND expiration_date = '9999-12-31 23:59:59'
  AND user_sk = 1;


-- Another Example of SCD Type 2
-- First, expire the current record for Jane Smith
UPDATE nessie.warehouse.dim_user_type2
SET expiration_date = TIMESTAMP '2023-06-15 00:00:00',
    is_current = false,
    updated_at = TIMESTAMP '2023-06-15 00:00:00'
WHERE user_id = '616700523341'
  AND is_current = true;

-- Insert the new record with updated information
INSERT INTO nessie.warehouse.dim_user_type2(
    user_sk,
    user_id,
    first_name,
    last_name,
    email,
    user_type,
    grade_level,
    school_id,
    effective_date,
    expiration_date,
    is_current,
    created_at,
    updated_at
) VALUES (
    -- Assuming next available surrogate key is 7
    7,                                          -- new surrogate key
    '616700523341',                            -- same user_id (natural key)
    'Jane',                                    -- same first name
    'Smith',                                   -- same last name
    'jane.smith.2023@newschool.edu',           -- new email
    'student',                                 -- same user type
    '11',                                      -- new grade level (promoted from 10)
    'SCH003',                                  -- new school
    TIMESTAMP '2023-06-15 00:00:00',           -- start date (same as previous record's end date)
    TIMESTAMP '9999-12-31 23:59:59',           -- end date (set to far future)
    true,                                      -- this is now the current record
    TIMESTAMP '2023-06-15 00:00:00',           -- when this record was created
    TIMESTAMP '2023-06-15 00:00:00'            -- when this record was last updated
);

-- To verify the changes, you can run this query:
SELECT
    user_id,
    first_name,
    last_name,
    email,
    grade_level,
    school_id,
    effective_date,
    expiration_date,
    is_current
FROM nessie.warehouse.dim_user_type2
WHERE user_id = '616700523341'
ORDER BY effective_date;