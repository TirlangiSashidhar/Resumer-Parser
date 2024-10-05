--CREATING THE DATATBASE
CREATE DATABASE RP_DATA;
USE RP_DATA;

-- CHECKING FOR THE CREATED TABLES
SELECT TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'

-- DISPLAYING THE CONTENT OF TABLES
SELECT * FROM [RP_DATA_CSV];

-- SQL TRANSACTION THAT INSERTS NEW RESUME DATA INTO THE DATABASE.

BEGIN TRANSACTION;

BEGIN TRY
    -- INSERT INTO THE RP_DATA_CSV TABLE WITH RELEVANT COLUMNS
    INSERT INTO RP_DATA_CSV (name, email, phone, skills, experience, education)
    VALUES 
    ('John Doe', 'john.doe@example.com', '123-456-7890', 'Python, SQL, Power BI', '3 years as Data Analyst', 'BSc Computer Science'),
    ('Jane Smith', 'jane.smith@example.com', '987-654-3210', 'Java, Spring, Hibernate', '5 years as Software Developer', 'MSc Software Engineering');
    
    -- COMMIT THE TRANSACTION IF ALL INSERTS ARE SUCCESSFUL
    COMMIT;
END TRY

BEGIN CATCH
    -- IN CASE OF AN ERROR, ROLLBACK THE TRANSACTION
    ROLLBACK;

    -- PRINT AN ERROR MESSAGE OR LOG THE ERROR
    PRINT 'An error occurred, transaction rolled back.';
    
    -- ERROR HANDLING,
END CATCH;




-- CREATE A SQL TRIGGER THAT AUTOMATICALLY LOGS THE INSERTION OF NEW RESUME DATA INTO A SEPARATE LOG TABLE
ALTER TABLE RP_DATA_CSV
ADD id INT IDENTITY(1,1) PRIMARY KEY;


CREATE TABLE resume_log (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    resume_id INT,
    insert_time DATETIME DEFAULT GETDATE()
);

CREATE TRIGGER trg_LogResumeInsert
ON RP_DATA_CSV
AFTER INSERT
AS
BEGIN
    -- INSERT INTO THE LOG TABLE WHEN A NEW RESUME IS ADDED
    INSERT INTO resume_log (resume_id, insert_time)
    SELECT inserted.id, GETDATE()
    FROM inserted;
END;




-- STORED PROCEDURE  THAT ACCEPTS MULTIPLE RESUME RECORDS AND INSERTS THEM INTO THE DATABASE IN A SINGLE CALL.
CREATE PROCEDURE InsertResumes
    @records NVARCHAR(MAX)
AS
BEGIN
    BEGIN TRANSACTION;

    BEGIN TRY
        -- SPLIT THE INPUT STRING INTO INDIVIDUAL RECORDS USING STRING_SPLIT
        DECLARE @resume NVARCHAR(MAX);
        DECLARE @resumeList TABLE (
            Name NVARCHAR(255),
            Email NVARCHAR(255),
            Phone NVARCHAR(255),
            Skills NVARCHAR(MAX),
            Experience NVARCHAR(MAX),
            Education NVARCHAR(255)
        );

        DECLARE @delimiter CHAR(1) = '#';
        DECLARE @fieldDelimiter CHAR(1) = ',';

        DECLARE cursor_insert CURSOR FOR 
            SELECT value FROM STRING_SPLIT(@records, @delimiter);

        OPEN cursor_insert;
        FETCH NEXT FROM cursor_insert INTO @resume;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- SPLIT INDIVIDUAL FIELDS
            DECLARE @name NVARCHAR(255);
            DECLARE @email NVARCHAR(255);
            DECLARE @phone NVARCHAR(255);
            DECLARE @skills NVARCHAR(MAX);
            DECLARE @experience NVARCHAR(MAX);
            DECLARE @education NVARCHAR(255);

            SET @name = PARSENAME(REPLACE(@resume, @fieldDelimiter, '.'), 6);
            SET @email = PARSENAME(REPLACE(@resume, @fieldDelimiter, '.'), 5);
            SET @phone = PARSENAME(REPLACE(@resume, @fieldDelimiter, '.'), 4);
            SET @skills = PARSENAME(REPLACE(@resume, @fieldDelimiter, '.'), 3);
            SET @experience = PARSENAME(REPLACE(@resume, @fieldDelimiter, '.'), 2);
            SET @education = PARSENAME(REPLACE(@resume, @fieldDelimiter, '.'), 1);

            -- INSERT INTO TABLE
            INSERT INTO Resumes (Name, Email, Phone, Skills, Experience, Education)
            VALUES (@name, @email, @phone, @skills, @experience, @education);

            FETCH NEXT FROM cursor_insert INTO @resume;
        END;

        CLOSE cursor_insert;
        DEALLOCATE cursor_insert;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;


