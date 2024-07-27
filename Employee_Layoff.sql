-- Data Cleaning--

select * 
from layoffs;
 
 -- 1. Remove Duplicate
 -- 2. standardize the data
 -- 3. null values or blank values
 -- 4. remove any columns

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

select *,
Row_number() over(
partition by company, industy, total_laid_off, percentage_laid_off,'date') as Row_num
from layoffs_staging;

with duplicate_cte as
( select *,
Row_number() over(
partition by company, location,
industy, total_laid_off, percentage_laid_off,'date',stage,
counry, funds_raised_millions) AS row_num
from layoffs_staging)

select *
from dulplicate_cte
where row_num > 1;

select *
from layoffs_staging
where company = 'casper';

with duplicate_cte as
( 
select *,
Row_number() over(
partition by company, location, industry, total_laid_off,
percentage_laid_off,'date', stage
,country, funds_raised_millions) as row_num
from layoffs_staging
)
delete 
from dulplicate_cte
where row_num > 1;


create table `layoffs_staging3`(
`company` text,
`location` text,
`industry` text,
`total_laid_off` int default null,
`percentage_laid_off` text,
`date` text,
`stage` text,
`country` text,
`funds_raised_millions` int default null,
`row_num`int
)
engine=InnoDB default charset=utf8mb4 
collate=utf8mb4_0900_ai_ci; 

select *
from layoffs_staging3
where row_num > 1;


insert into layoffs_staging3
select*,
Row_number() over(
partition by company, location, industry, total_laid_off,
percentage_laid_off,'date', stage
,country, funds_raised_millions) as row_num
from layoffs_staging;


delete 
from layoffs_staging3
where row_num > 1;


select company, trim(company)
from layoffs_staging3;

-- standardizing data
 select company , trim(company)
 from layoffs_staging3;

update layoffs_staging3
set company = trim(company);

 select distinct industry
 from layoffs_staging3;

 
 update layoffs_staging3
 set industry = 'crypto'
 where industry like 'crypto%';
 
 select distinct country , trim(trailing '.' from country)
 from layoffs_staging3
 order by 1;

update layoffs_staging3
set country = trim(trailing ',' from country)
where country like 'United States%';

select `date`
from layoffs_staging3;

update layoffs_staging3
set `date` = str_to_date(`date`,'%m/%d/%Y');

alter table layoffs_staging3
modify column `date` date;


select *
from layoffs_staging
where total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging3
set industry = null
WHERE INDUSTRY = '';

 select *
 from layoffs_staging3
 where industry is null
 or industry = '';
 
 select *
 from layoffs_staging3
 where company LIKE 'BALLY%';
 
 
 select t1.industry, t2.industry
 from layoffs_staging3 t1
 join layoffs_staging3 t2
   on t1.company = t2.company
   where (t1.industry is null or t1.industry = '')
   and t2.industry is not null;
   
   
   update layoffs_staging3 t1
   join layoffs_staging3 t2
    on t1.company = t2.company
    set t1.industry = t2.industry
    where t1.industry is null 
   and t2.industry is not null;
   
   
 select *
 from layoffs_staging3;
 
 select *
from layoffs_staging3
where total_laid_off is null
and percentage_laid_off is null;


DELETE
from layoffs_staging3
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging3;

ALTER table layoffs_staging3
DROP column row_num;