use project_11;

create table layoffs_satgging
like layoffs;

insert into layoffs_stagging
select * from layoffs;

select * from  layoffs_stagging;

create table layoffs_stagging_1
like layoffs_stagging;

insert into layoffs_stagging_1 --( duplicate table) --
select * from layoffs_stagging;

-- remove duplicate --
select company,industry, total_laid_off, `date`,
row_number() 
over (partition by company, industry, total_laid_off, `date`) 
as row_num
from 
layoffs_stagging ;

select * 
from (select company,industry, total_laid_off, `date`,
row_number() 
over (partition by company)
/*, industry, total_laid_off, `date`--)*/ 
as row_num
from 
layoffs_stagging )duplicates
where
 row_num > 1;
 
 select * from layoffs  where company = "Indigo"
 SELECT *

FROM (

	SELECT company, industry, total_laid_off,`date`,

		ROW_NUMBER() OVER (

			PARTITION BY company, industry, total_laid_off,`date`

			) AS row_num

	FROM 

		layoffs_stagging

) duplicates

WHERE 

	row_num > 1;
 -- thses are our real duplicates
 select * from (
 select company, location, industry, total_laid_off,
 percentage_laid_off, `date` , stage, country, funds_raised_millions,
 row_number() over (partition by company, location, industry, total_laid_off,
 percentage_laid_off, `date` , stage, country, funds_raised_millions)
 as row_num 
 from layoffs_stagging) duplicates 
 where 
 row_num > 1;
 
 /* SELECT *
FROM (
    SELECT 
        company, 
        industry, 
        total_laid_off, 
        ⁠ `date` ⁠
        ROW_NUMBER() OVER (
            PARTITION BY company, industry, total_laid_off, ⁠ `date` ⁠
            ORDER BY company
        ) AS row_num
    FROM layoffs_stagging
) duplicates
WHERE row_num > 1;
select version() */

SELECT *
FROM (
    SELECT 
        company, 
        industry, 
        total_laid_off, 
        `date`,
        ROW_NUMBER() OVER (
            PARTITION BY company, industry, total_laid_off, `date`
            ORDER BY company
        ) AS row_num
    FROM layoffs_stagging
) AS duplicates
WHERE row_num > 1;

SELECT company, industry, total_laid_off, `date`, COUNT(*)
FROM layoffs_stagging
GROUP BY company, industry, total_laid_off, `date`
HAVING COUNT(*) > 1;

-- these are the ones we want to delete where the row number is > 1 or 
-- 2 or greater essentially
 want to write it like this:
 WITH delete_cte AS (
    SELECT *
    FROM (
        SELECT 
            company, location, industry, total_laid_off, percentage_laid_off, 
            `date`, stage, country, funds_raised_millions,
            ROW_NUMBER() OVER (
                PARTITION BY company, location, industry, total_laid_off, 
                             percentage_laid_off, `date`, stage, country, 
                             funds_raised_millions
                ORDER BY company
            ) AS row_num
        FROM layoffs_stagging
    ) AS duplicates
    WHERE row_num > 1
)select 
from 
delete_cte;

-- one solution, which i think is a good one. Is to create a new column 
-- and add those row numbers in . Then delete where row numbers are over 2,
-- then delete that column 
-- so lets's do it !!

alter table layoffs_stagging add row_num int;

CREATE TABLE `layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);
 
INSERT INTO `layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,
            percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_stagging;
 
-- now that we have this we can delete rows were row_num is greater than 2

delete from layoffs_staging2
where row_num >= 2;

set sql_safe_updates = 0;

-- 2. standarlize data 

select * from layoffs_staging2;

-- if we look at industry it looka like we have some null
-- and empty row, lets take a look at these

select distinct industry
from layoffs_staging2
order by industry;

select * 
from layoffs_staging2
where industry is null
or industry = ''
order by industry ; 
-- day 1 -- project --29-07
-- lets take a look at these
select * 
from layoffs_staging2
where company like 'Bally%';
-- nothing wrong here 
select * from layoffs_staging2
where company like 'airbnb%';

-- it looks like airbnb is a travel, but this one just isn't populated.
-- i'm sure it;s the same for the others. What we can do is
-- write a query that if there is another row with the same company name,
-- it will update it to the non-null industry values
-- make it easy so if there were thousand we wouldn't have to manually check them all

-- wer should set the blanks to null since those
-- are typically easier to work with
update layoffs_staging2
set industry = null
where industry = '';

-- now if we check those are all null

select *
 from layoffs_staging2
 where industy is null
 or industry = ''
 order by industry;
 
 -- now we need to populate those null if possible 
 
 update layoffs_staging2 t1
 join layoffs_staging2 t2
 on t1.company = t2.company 
 set t1.industry = t2.industry
 where t1.industry is null
 and t2.industry is not null;
 
 -- and if we check it looks like Bally's was
 -- the only one without a populated row to populate this null values
 select *
 from layoffs_staging2
 where industry is null
 or industry = ''
 order by industry ;
 ---------------------------- ---  ---- ----

-- i also noticed that Crypto has multiple differnet variations.
-- we need to standardize that - let's say all to Crypto



update layoffs_staging2
set industry ='Crypto'
where industry in('Crypto' , 'CryptoCurrency');

-- now that's taken care of:
select distinct industry 
from layoffs_staging2
order by industry;

--------------------
we also need to look at 

select * from layoffs_staging2;
-- eveything looks good except apparently
-- we have some 'United States' some 'United States.'
-- with a period at the end. lets standardize this too.

select distinct country 
from layoffs_staging2
order by country;

update layoffs_staging2
set country = trim(trailing '.' from country);  -- This query will
-- successfully remove any trailing periods from the country column.

-- now if we run this agin it is fixed
select distinct country 
from layoffs_staging2
order by country ;

-- lets also fix teh date column :
select *
from layoffs_staging2;

-- we can str to date to update this fied
-- update layoffs_staging2
set `date` = str_to_date(`date` , '%m/%d/%Y')

-- now we can convert tah date type properly 
update layoffs_staging2
set `date` =
case 
-- handle M/D/YYYY or MM/DD/YYYY
when `date` regexp '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
then str_to_date(`date`, '%m/%d/%Y')

-- handle YYYY-MM-DD
when `date` regexp '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
then `date`
-- null or invalid stays null
else null 
end;

select * from layoffs_staging2;
SHOW WARNINGS;
LOAD DATA INFILE 'file.csv'
INTO TABLE your_table
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;
SHOW ERRORS;

-- EDA
 
-- Here we are jsut going to explore the data and find trends or patterns or anything interesting like outliers
-- outlier means : A= (1,2,3,4,5,6,7,8,9,100) here outlier is 100
 
-- normally when you start the EDA process you have some idea of what you're looking for
 
-- with this info we are just going to look around and see what we find! 
 
 -- 3. Look at Null Values
 
-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions 
--- all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase
 
-- so there isn't anything I want to change with the null values
 
 
 -- 4. remove any columns and rows we need to

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- Delete Useless data we can't really use
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT * 
FROM layoffs_staging2;


ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


SELECT * 
FROM layoffs_staging2;

SELECT MAX(total_laid_off) FROM layoffs_taging2;

SELECT MAX(total_laid_off)
FROM layoffs_staging2;


-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM layoffs_staging2
WHERE  percentage_laid_off = 1;
-- these are mostly startups it looks like who all went 
-- out of business during this time


-- if we order by funds_raised_millions we can see
-- how big some of these companies were
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;


-- BritishVolt looks like an EV company, Quibi! 
-- I recognize that company - wow raised like 2 billion 
-- dollars and went under - ouch

 
-- SOMEWHAT TOUGHER AND MOSTLY USING GROUP BY---------------------------------------------

-- Companies with the biggest single DAY Layoff
SELECT company, total_laid_off
FROM layoffs_staging
ORDER BY 2 DESC
LIMIT 5;
-- now that's just on a single day

-- Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;


-- BY LOCATION
SELECT location, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- THIS IS TOTAL IN THE PAST 3 YEARS OR IN THE DATASET
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;
 
SELECT YEAR(date), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 ASC;
 
 
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;
 
 
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 asc;
  
 
WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY 
  total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- rollimg total of layoffs per month 
select substring (date, 1,7) as dates , sum(total_laid_off) as total_laid_off
 from  layoffs_staging2
 group by dates 
 order by dates asc;
 
-- now use it in cte so we can query  off of it
 
 WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;
 
 