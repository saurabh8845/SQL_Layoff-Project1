
use world_layoffs;

select * from layoffs;

-- 1. remove duplicate
-- 2. standardize the data
-- 3. null values or nlank values
-- 4. remove any columns



create table layoffs_staging
like layoffs;

select * from layoffs_staging;

insert layoffs_staging 
select * from layoffs;

select *from layoffs_staging;

select * ,
ROW_NUMBER() OVER(partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;

with duplicate_cte as (select * ,
ROW_NUMBER() OVER(partition by company,location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging)
select * from duplicate_cte
where row_num > 1;

create table layoffs_staging2(
 company text,
 location text,
 industry text, 
 total_laid_off int default null,
 percentage_laid_off text, 
 `date` text,
 stage text, country text, funds_raised_millions int default null, row_num int);
 
 select * from layoffs_staging2;
insert into layoffs_staging2
select * ,
ROW_NUMBER() OVER(partition by company,location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

select * from layoffs_staging2
where row_num > 1;

delete from layoffs_staging2
where row_num > 1;

select * from layoffs_staging2;


-- standarize data --finding issue in data and fix it

select distinct(company) from 
layoffs_staging2;

select company, trim(company)
from layoffs_staging2;

-- safe mode off
use world_layoffs;

update layoffs_staging2
set company = trim(company);

select industry from layoffs_staging2
order by 1;

-- changing same of same country from different

select * from layoffs_staging2
where industry like "Crypto%";

update layoffs_staging2 
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct(industry) from layoffs_staging2;


-- looking into location

select distinct(location)
from layoffs_staging2;

select distinct(country) from layoffs_staging2;

-- we have find (.) after the united state so we gone trim it by the help of trailing function

select distinct(country), trim(trailing '.' from country)from layoffs_staging2;

update layoffs_staging
set country = trim(trailing '.' from country)
where country like 'United States%';




-- changing date from text to date

select date from layoffs_staging;
select `date`,
str_to_date(`date`, '%m/%d/%y') from layoffs_staging;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');


alter table layoffs_staging2
modify column `date` date;


-- blank values or null

select  * from layoffs_staging2
where total_laid_off is null;

select * from layoffs_staging2
where industry is null
or industry = '';


select * from layoffs_staging2
where company = 'Airbnb';

update layoffs_staging2
set industry = null
where industry =''; 

select t1.industry, t2.industry from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
and t1.location = t2.location
where (t1.industry is null or t1.industry='') and t2.industry is not null;





update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company= t2.company
set t1.industry= t2.industry
where t1.industry is null 
 and t2.industry is not null;
 
 
 -- for null values
 delete 
 from layoffs_staging2 
 where total_laid_off is null
 and percentage_laid_off is null;
 
 -- drop column
 
 alter table layoffs_staging2
 drop column row_num;
 
 use world_layoffs;
 
 select * from layoffs_staging2;
 
 

 
 
 
 
 
 
 
 -- Exploratory Data Analysis
 
 select * from layoffs_staging2;

 select max(total_laid_off), max(percentage_laid_off)
 from layoffs_staging2;
 
 select *
 from layoffs_staging2 where percentage_laid_off =1
order by stage;


select company, sum(total_laid_off)
from layoffs_staging2
group by company;

select min(`date`), max(`date`)
from layoffs_staging2;

select * from layoffs_staging2;
select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2;

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2;


select substring(`date`,6,2) as `month`, sum(total_laid_off)
from layoffs_staging2
group by `month`
order by 1 asc;


with rolling_total as
(
  select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)
select `month`, sum(total_off) over(order by `month`)
from rolling_total;

with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2
group by company, year(`date`)
), company_year_rank as(
select * , dense_rank() over (partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null)
select *from company_year_rank
where ranking <=5
commit layoffs_staging