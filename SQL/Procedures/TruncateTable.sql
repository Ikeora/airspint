-- Truncate a table
CREATE PROCEDURE TruncateTable
    @TableName NVARCHAR(128)
AS
BEGIN
    DECLARE @SQL NVARCHAR(MAX);
    SET @SQL = 'TRUNCATE TABLE ' + QUOTENAME(@TableName);
    EXEC sp_executesql @SQL;
END