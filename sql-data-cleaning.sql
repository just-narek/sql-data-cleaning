-- Remove Duplicates
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, industry, total_laid_off) as row_num
FROM layoffs_staging;

WITH duplicates_CTE AS (
		
	SELECT *, 
	ROW_NUMBER()  OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) as row_num
	FROM layoffs_staging

)
SELECT *
FROM duplicates_CTE
WHERE row_num > 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
	
	SELECT *, 
	ROW_NUMBER()  OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
	FROM layoffs_staging;


DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0;
DELETE FROM layoffs_staging2 WHERE row_num > 1;
SET SQL_SAFE_UPDATES = 1;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;




-- Standardizing data

SELECT company, TRIM(company)
FROM layoffs_staging2;




UPDATE layoffs_staging2
SET company = TRIM(company);

UPDATE layoffs_staging2
SET location = TRIM(location);

UPDATE layoffs_staging2
SET industry = TRIM(industry);

UPDATE layoffs_staging2
SET country =TRIM(country) ;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';


UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE "United StaT%";


UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE; 
 

UPDATE layoffs_staging2
SET industry = 'X'
WHERE industry IS NULL;

UPDATE layoffs_staging2
SET industry = 'X'
WHERE industry = '';

UPDATE layoffs_staging2
SET stage = 'X'
WHERE stage IS NULL;


DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
AND funds_raised_millions IS NULL;



DELETE 
FROM layoffs_staging2
WHERE total_laid_off = 0;

DELETE 
FROM layoffs_staging2
WHERE percentage_laid_off = 0;

DELETE 
FROM layoffs_staging2
WHERE funds_raised_millions = 0;

SELECT * FROM layoffs_staging2 ;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * FROM layoffs_staging2 ;
