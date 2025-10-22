
-- Manual schema helper if not using Python
CREATE TABLE BrandDetail (
    BrandID        INT           NOT NULL PRIMARY KEY,
    BrandName      NVARCHAR(255) NULL,
    URL            NVARCHAR(512) NULL,
    Category       NVARCHAR(255) NULL,
    Subcategory    NVARCHAR(255) NULL
);

CREATE TABLE DailySpend (
    [Date]        DATE          NOT NULL,
    BrandID       INT           NOT NULL,
    [State]       NVARCHAR(50)  NOT NULL,
    Spend         DECIMAL(18,2) NULL,
    Transactions  INT           NULL,
    CONSTRAINT PK_DailySpend PRIMARY KEY ([Date], BrandID, [State]),
    CONSTRAINT FK_DailySpend_BrandDetail FOREIGN KEY (BrandID) REFERENCES BrandDetail(BrandID)
);
