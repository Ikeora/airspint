-- Load CSV file from Azure Blob Storage to SQL Server table
-- https://docs.microsoft.com/en-us/sql/t-sql/statements/bulk-insert-transact-sql?view=sql-server-ver15

CREATE PROCEDURE LoadCSVToSQL
    @FileName NVARCHAR(255),
    @TableName NVARCHAR(255)
AS
BEGIN
    DECLARE @SQL NVARCHAR(MAX);

    SET @SQL = '
    BULK INSERT ' + QUOTENAME(@TableName) + '
    FROM ''https://airsprintsa.blob.core.windows.net/cleandata/' + @FileName + '''
    WITH (
        DATA_SOURCE = ''MyBlobStorage'',
        FORMAT = ''CSV'',
        FIRSTROW = 2,
        FIELDTERMINATOR = '','',
        ROWTERMINATOR = ''\n'',
        TABLOCK
    );';

    EXEC sp_executesql @SQL;
END;