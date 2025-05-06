-- Exploratory Data Analysis (EDA)

-- In this section, we'll explore the dataset to uncover trends, patterns, and potential outliers.

-- Typically, EDA begins with a specific objective or hypothesis in mind, 
-- but here we'll take an open-ended approach to identify any interesting insights.

-- Let's begin by examining the data and observing any notable findings.

SELECT * 
FROM world_layoffs.layoffs_staging2;

-- EASIER QUERIES

SELECT MAX(total_laid_off)
FROM world_layoffs.layoffs_staging2;

-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- EXPLORATORY DATA ANALYSIS (EDA) ON TECH LAYOFFS

-- 1. Exploring the magnitude of layoffs by percentage
SELECT MAX(percentage_laid_off) AS max_layoff_pct,
       MIN(percentage_laid_off) AS min_layoff_pct
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off IS NOT NULL;

-- 2. Companies that laid off 100% of their workforce
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1;

-- 3. Sorting those companies by funds raised to assess scale
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised DESC;

-- Observations:
-- Several high-profile or well-funded startups like Quibi and BritishVolt went out of business.

----------------------------------------------------------------------------------------------------
-- AGGREGATED INSIGHTS USING GROUP BY

-- 4. Largest single-day layoffs by company
SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging
ORDER BY total_laid_off DESC
LIMIT 5;

-- 5. Companies with the highest total layoffs
SELECT company, SUM(total_laid_off) AS total_layoffs
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY total_layoffs DESC
LIMIT 10;

-- 6. Layoffs by location
SELECT location, SUM(total_laid_off) AS total_layoffs
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY total_layoffs DESC
LIMIT 10;

-- 7. Layoffs by country
SELECT country, SUM(total_laid_off) AS total_layoffs
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY total_layoffs DESC;

-- 8. Layoffs by year
SELECT YEAR(date) AS layoff_year, SUM(total_laid_off) AS total_layoffs
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(date)
ORDER BY layoff_year;

-- 9. Layoffs by industry
SELECT industry, SUM(total_laid_off) AS total_layoffs
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY total_layoffs DESC;

-- 10. Layoffs by funding stage
SELECT stage, SUM(total_laid_off) AS total_layoffs
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY total_layoffs DESC;

----------------------------------------------------------------------------------------------------
-- ADVANCED ANALYSIS

-- 11. Top 3 companies with the most layoffs per year
WITH Company_Year AS (
  SELECT company, YEAR(date) AS year, SUM(total_laid_off) AS total_layoffs
  FROM world_layoffs.layoffs_staging2
  GROUP BY company, YEAR(date)
),
Company_Year_Rank AS (
  SELECT company, year, total_layoffs,
         DENSE_RANK() OVER (PARTITION BY year ORDER BY total_layoffs DESC) AS company_rank
  FROM Company_Year
)
SELECT company, year, total_layoffs, company_rank
FROM Company_Year_Rank
WHERE company_rank <= 3 AND year IS NOT NULL
ORDER BY year, total_layoffs DESC;


-- 12. Monthly rolling total of layoffs
WITH Monthly_Layoffs AS (
  SELECT SUBSTRING(date, 1, 7) AS month, SUM(total_laid_off) AS monthly_total
  FROM world_layoffs.layoffs_staging2
  GROUP BY month
)
SELECT month,
       SUM(monthly_total) OVER (ORDER BY month) AS rolling_total_layoffs
FROM Monthly_Layoffs
ORDER BY month;
