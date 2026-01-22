select *
from bonuscheques b 


-- RFM-подготовка данных
-- Извлечение и агрегация метрик Recency, Frequency, Monetary по клиентам
with rfm_raw as (
    select
        card as customer_id,
        max(datetime::date) as last_purchase_date,
        current_date - max(datetime::date) as recency_days,
        count(*) as purchase_count,
        sum(summ) as total_spent
    from bonuscheques
    where card is not null and card like '2000%'
    group by card
),
stats as (
    select
        min(recency_days) as min_recency,
        max(recency_days) as max_recency
    from rfm_raw
),
rfm_data as (
    select
        r.customer_id,
        r.last_purchase_date,
        r.recency_days,
        1 + (r.recency_days - s.min_recency) * 332.0 / nullif(s.max_recency - s.min_recency, 0) as norm_recency,
        r.purchase_count,
        r.total_spent
    from rfm_raw r
    cross join stats s
)
select *
from rfm_data;


-- Анализируем распределение метрики Recency (давности последней покупки) по клиентам.
with rfm_raw as (
    select 
        (current_date - max(datetime::date)) as recency_days  -- количество дней с момента последней покупки
    from bonuscheques
    where card is not null and card like '2000%'
    group by card 
),
stats as (
    select
        min(recency_days) as min_recency,
        max(recency_days) as max_recency
    from rfm_raw
),
rfm_normalized as (
    select
        recency_days,
        1 + (recency_days - min_recency) * (332 - 1) / (max_recency - min_recency) as norm_recency
    from rfm_raw, stats
)
select
    count(*) as total_customers,  -- считаем количество строк
    min(norm_recency) as min_recency_norm,   -- будет ~1.0 -- минимальное значение дней с последней покупки
    max(norm_recency) as max_recency_norm,   -- будет ~332.0 -- максимальное значение дней с последней покупки
    avg(norm_recency) as avg_recency_norm, -- среднее количество дней с последней покупки по всем клиентам
    percentile_cont(0.5) within group (order by norm_recency) as median_recency_norm, -- медиана
    percentile_cont(0.25) within group (order by norm_recency) as q1_recency_norm, -- первый квартиль
    percentile_cont(0.75) within group (order by norm_recency) as q3_recency_norm -- третий квартиль
from rfm_normalized


-- Анализ распределения метрики Frequency (количество покупок) по клиентам.
select
    count(*) as total_customers,  -- считаем количество строк
    min(quantity) as min_freq, -- минимальное количество покупок по клиентам
    max(quantity) as max_freq, -- максимальное количество покупок по клиентам
    avg(quantity) as avg_freq, -- среднее количество покупок по клиентам
    percentile_cont(0.5) within group (order by quantity) as median_freq, -- находим медиану
    percentile_cont(0.25) within group (order by quantity) as q1_freq, -- первый квартиль
    percentile_cont(0.75) within group (order by quantity) as q3_freq -- третий квартиль
from (
    select 
        card as customer_id,
        count(*) as quantity
    from bonuscheques
    where card is not null and card like '2000%'
    group by card 
) rfm 


-- Анализ распределения метрики Monetary(траты) по клиентам.
select
    count(*) as total_customers,
    min(monetary) as min_mon, -- минимальные траты
    max(monetary) as max_mon, -- максимальные траты
    avg(monetary) as avg_mon, -- средние траты
    percentile_cont(0.5) within group (order by monetary) as median_mon,
    percentile_cont(0.25) within group (order by monetary) as q1_mon,
    percentile_cont(0.75) within group (order by monetary) as q3_mon
from (
    select 
        card as customer_id,
        sum(summ) as monetary
    from bonuscheques
    where card is not null and card like '2000%'
    group by card 
) rfm


-- ABC-анализ по метрике Monetary (общая сумма покупок клиента)
with rfm_raw as (
    select
        card as customer_id,
        max(datetime::date) as last_purchase_date, -- дата последней покупки
        sum(summ) as monetary -- общая сумма всех покупок 
    from bonuscheques
    where card is not null and card like '2000%'
    group by card -- группируем по карте(customer_id)
    ),
monetary_ranks as ( -- для каждого клиента
    select 
        customer_id,
        monetary,
        sum(monetary) over () as total_revenue, -- сумма всех monetary по всем клиентам.
        sum(monetary) over (order by monetary desc rows unbounded preceding) as cum_revenue -- сумма monetary всех клиентов, отсортированных по monetary в порядке убывания
    from rfm_raw
)
select *,
    case 
        when cum_revenue <= 0.8 * total_revenue then 'A' -- Клиенты, чья накопленная выручка до 80%
        when cum_revenue <= 0.95 * total_revenue then 'B' -- Клиенты, чья накопленная выручка до 95% (включая тех, кто уже в A)
        else 'C'
    end as abc_class
from monetary_ranks



--В классических описаниях RFM часто используется 3-уровневая градация (High / Medium / Low),
--выбор 5 уровней обусловлен следующими соображениями: 
--1)объем выборки больше 6000 клиентов, позволяет проводить более тонкую сегментацию
--2)5-балльная шкала даёт 125 уникальных RFM-кодов (5×5×5), что позволяет выявлять узкие, но ценные подгруппы (например, «активные, но мало тратящие»)
--3)5-уровневая RFM-шкала широко применяется в ритейле, e-commerce и финансовых сервисах при работе с базами от нескольких тысяч клиентов и выше. Это де-факто отраслевой стандарт для бизнесов, стремящихся к персонализации и повышению LTV.

with rfm_raw as (  -- Присвоение RFM-баллов
    select
        card as customer_id, -- Уникальный идентификатор клиента (карта)
        max(datetime::date) as last_purchase_date, -- дата последней покупки клиента
        (current_date - max(datetime::date)) - 1273 as recency_days, -- сколько дней прошло с последней покупки (Recency)
        count(*) as quantity, -- количество покупок клиента (Frequency)
        sum(summ) as monetary -- общая сумма покупок клиента (Monetary)
    from bonuscheques -- таблица с чеками
    where card is not null and card like '2000%' -- фильтрация: карта не пустая и начинается с '2000'
    group by card -- группировка по карте (клиенту)
),
rfm_scored as (
    select
        customer_id,
        recency_days,
        quantity,
        monetary,
		  case -- R: активность (чем меньше дней — тем выше балл)
            when recency_days <= 60 then 5  -- очень активные
            when recency_days <= 120 then 4 -- активные
            when recency_days <= 180 then 3 -- средняя активность
            when recency_days <= 365 then 2 -- спящие
            else 1                         -- давно не были
        end as r_score,       
        case-- F: частота (чем больше покупок — тем выше балл)
            when quantity >= 10 then 5     -- частые покупатели
            when quantity >= 5 then 4      -- часто
            when quantity >= 3 then 3      -- иногда
            when quantity >= 2 then 2      -- редко
            else 1                         -- одноразовые
        end as f_score,                  
        case -- M: сумма (чем больше — тем выше балл)
            when monetary >= 5000 then 5   -- крупные траты
            when monetary >= 2500 then 4   -- высокие
            when monetary >= 1000 then 3   -- средние
            when monetary >= 500 then 2    -- низкие
            else 1                         -- минимальные
        end as m_score
    from rfm_raw
),
rfm_final as (
    select
        customer_id,
        recency_days,
        quantity,
        monetary,
        r_score,
        f_score,
        m_score,
        cast(r_score as text) || cast(f_score as text) || cast(m_score as text) as rfm_code,-- Объединяем баллы в один RFM-код (например, "543")
        -- Сегментация клиентов по RFM-баллам
        case
             when r_score >= 4 and f_score >= 3 and m_score >= 3 then 'Лучшие клиенты' -- активные, часто покупают, много тратят
            when r_score <= 2 and (f_score >= 4 or m_score >= 4) then 'Спящие клиенты (высокая ценность)'
            when r_score <= 2 and (f_score >= 3 or m_score >= 3) then 'Спящие клиенты (средняя ценность)' -- давно не были, но раньше активно тратили/покупали
            when r_score >= 4 and f_score >= 2 and m_score = 3 then 'Активные, но мало тратящие' -- часто, но мало
            when r_score >= 4 and f_score >= 2 and m_score < 3  then 'Потенциальные клиенты'
            when quantity = 1 and monetary >= 1000 then 'Одноразовые (высокий потенциал)'
            when quantity = 1 and monetary < 1000  then 'Одноразовые (низкий потенциал)' -- купили один раз и мало
            else 'Средние клиенты' -- все остальные
        end as segment
    from rfm_scored
)
select *
from rfm_final
order by monetary desc -- сортировка по общей сумме покупок по убыванию;



with rfm_raw as (  -- Присвоение RFM-баллов
    select
        card as customer_id, -- Уникальный идентификатор клиента (карта)
        max(datetime::date) as last_purchase_date, -- дата последней покупки клиента
        (current_date - max(datetime::date)) -1273 as recency_days, -- сколько дней прошло с последней покупки (Recency)
        count(*) as quantity, -- количество покупок клиента (Frequency)
        sum(summ) as monetary -- общая сумма покупок клиента (Monetary)
    from bonuscheques -- таблица с чеками
    where card is not null and card like '2000%' -- фильтрация: карта не пустая и начинается с '2000'
    group by card -- группировка по карте (клиенту)
),
rfm_scored as (
    select
        customer_id,
        recency_days,
        quantity,
        monetary,
        case -- R: активность (чем меньше дней — тем выше балл)
            when recency_days <= 60 then 5  -- очень активные
            when recency_days <= 120 then 4 -- активные
            when recency_days <= 180 then 3 -- средняя активность
            when recency_days <= 365 then 2 -- спящие
            else 1                         -- давно не были
        end as r_score,    
        case-- F: частота (чем больше покупок — тем выше балл)
            when quantity >= 10 then 5     -- частые покупатели
            when quantity >= 5 then 4      -- часто
            when quantity >= 3 then 3      -- иногда
            when quantity >= 2 then 2      -- редко
            else 1                         -- одноразовые
        end as f_score,                  
        case -- M: сумма (чем больше — тем выше балл)
            when monetary >= 5000 then 5   -- крупные траты
            when monetary >= 2500 then 4   -- высокие
            when monetary >= 1000 then 3   -- средние
            when monetary >= 500 then 2    -- низкие
            else 1                         -- минимальные
        end as m_score
    from rfm_raw
),
rfm_final as (
    select
        customer_id,
        recency_days,
        quantity,
        monetary,
        r_score,
        f_score,
        m_score,
        cast(r_score as text) || cast(f_score as text) || cast(m_score as text) as rfm_code,-- Объединяем баллы в один RFM-код (например, "543")
        -- Сегментация клиентов по RFM-баллам
        case
            when r_score >= 4 and f_score >= 3 and m_score >= 3 then 'Лучшие клиенты' -- активные, часто покупают, много тратят
            when r_score <= 2 and (f_score >= 4 or m_score >= 4) then 'Спящие клиенты (высокая ценность)'
            when r_score <= 2 and (f_score >= 3 or m_score >= 3) then 'Спящие клиенты (средняя ценность)' -- давно не были, но раньше активно тратили/покупали
            when r_score >= 4 and f_score >= 2 and m_score = 3 then 'Активные, но мало тратящие' -- часто, но мало
            when r_score >= 4 and f_score >= 2 and m_score < 3  then 'Потенциальные клиенты'
            when quantity = 1 and monetary >= 1000 then 'Одноразовые (высокий потенциал)'
            when quantity = 1 and monetary < 1000  then 'Одноразовые (низкий потенциал)' -- купили один раз и мало
            else 'Средние клиенты' -- все остальные
        end as segment
    from rfm_scored
),
monetary_ranks as ( -- ABC-анализ по Monetary
    select
        customer_id, -- Уникальный индификатор клиента
        monetary, -- Общая сумма покупок клиента       
        sum(monetary) over () as total_revenue, -- Общая выручка по всем клиентам (сумма всех monetary)
        sum(monetary) over (order by monetary desc rows unbounded preceding) as cum_revenue -- Накопленная выручка: суммируем monetary клиентов, отсортированных по убыванию monetary
        -- Это позволяет определить, какой вклад в общую выручку делает каждый клиент в порядке убывания
    from rfm_raw -- таблица с клиентами и их метриками (из предыдущего шага)
),
abc as (
    select
        customer_id,
        case 
            when cum_revenue <= 0.8 * total_revenue then 'a'  -- Если накопленная выручка (cum_revenue) <= 80% от общей — класс A (самые ценные клиенты)
            when cum_revenue <= 0.95 * total_revenue then 'b' -- Если <= 95% — класс B (средние по ценности)
            else 'c'  -- Остальные — класс C (наименее ценные)
        end as abc_class
    from monetary_ranks
),
final_enriched as ( -- Объединение RFM и ABC
    select
        f.*, -- Все колонки из RFM-анализа (customer_id, rfm_code, segment и т.д.)
        a.abc_class -- Добавляем ABC-класс к каждому клиенту
    from rfm_final f -- результаты RFM-сегментации
    join abc a on f.customer_id = a.customer_id
)
select -- Финальный отчёт: статистика по сегментам
    segment, -- RFM-сегмент клиента (например, "Лучшие клиенты", "Спящие" и т.д.)
    abc_class, -- ABC-класс клиента (a, b, c)
    count(*) as customers, -- количество клиентов в группе
    round(avg(monetary), 2) as avg_monetary, -- средняя сумма покупок в группе
    round(avg(recency_days), 1) as avg_recency, -- среднее количество дней с последней покупки
    round(avg(quantity), 1) as avg_frequency -- среднее количество покупок
from final_enriched
group by segment, abc_class -- группируем по RFM-сегменту и ABC-классу
order by avg_monetary desc; -- сортируем по средней сумме покупок по убыванию

-- RFM + ABC + norm_recency (с CURRENT_DATE)
with rfm_raw as (
    select
        card as customer_id,
        max(datetime::date) as last_purchase_date,
        current_date - max(datetime::date) as recency_days,  -- реальные дни с последней покупки
        count(*) as quantity,
        sum(summ) as monetary
    from bonuscheques
    where card is not null and card like '2000%'
    group by card
),
stats as (
    select
        min(recency_days) as min_recency,
        max(recency_days) as max_recency
    from rfm_raw
),
rfm_scored as (
    select
        r.customer_id,
        r.last_purchase_date,
        r.recency_days,
        -- Нормализация Recency: от 1 до 333
        1 + (r.recency_days - s.min_recency) * 332.0 / nullif(s.max_recency - s.min_recency, 0) as norm_recency,
        r.quantity,
        r.monetary,
        -- R-score на основе реальных дней (не norm_recency!)
        case
            when 1 + (r.recency_days - s.min_recency) * 332.0 / nullif(s.max_recency - s.min_recency, 0)  <= 60 then 5
            when 1 + (r.recency_days - s.min_recency) * 332.0 / nullif(s.max_recency - s.min_recency, 0) <= 120 then 4
            when 1 + (r.recency_days - s.min_recency) * 332.0 / nullif(s.max_recency - s.min_recency, 0)  <= 180 then 3
            when 1 + (r.recency_days - s.min_recency) * 332.0 / nullif(s.max_recency - s.min_recency, 0)  <= 365 then 2
            else 1
        end as r_score,
        case
            when r.quantity >= 10 then 5
            when r.quantity >= 5 then 4
            when r.quantity >= 3 then 3
            when r.quantity >= 2 then 2
            else 1
        end as f_score,
        case
            when r.monetary >= 5000 then 5
            when r.monetary >= 2500 then 4
            when r.monetary >= 1000 then 3
            when r.monetary >= 500 then 2
            else 1
        end as m_score
    from rfm_raw r
    cross join stats s
),
rfm_final as (
    select
        customer_id,
        recency_days,
        norm_recency,
        quantity,
        monetary,
        r_score,
        f_score,
        m_score,
        cast(r_score as text) || cast(f_score as text) || cast(m_score as text) as rfm_code,
        case
            when r_score >= 4 and f_score >= 3 and m_score >= 3 then 'Лучшие клиенты'
            when r_score <= 2 and (f_score >= 4 or m_score >= 4) then 'Спящие клиенты (высокая ценность)'
            when r_score <= 2 and (f_score >= 3 or m_score >= 3) then 'Спящие клиенты (средняя ценность)'
            when r_score >= 4 and f_score >= 2 and m_score = 3 then 'Активные, но мало тратящие'
            when r_score >= 4 and f_score >= 2 and m_score < 3 then 'Потенциальные клиенты'
            when quantity = 1 and monetary >= 1000 then 'Одноразовые (высокий потенциал)'
            when quantity = 1 and monetary < 1000 then 'Одноразовые (низкий потенциал)'
            else 'Средние клиенты'
        end as segment
    from rfm_scored
),
monetary_ranks as (
    select
        customer_id,
        monetary,
        sum(monetary) over () as total_revenue,
        sum(monetary) over (order by monetary desc, customer_id rows unbounded preceding) as cum_revenue
    from rfm_raw
),
abc as (
    select
        customer_id,
        case 
            when cum_revenue <= 0.8 * total_revenue then 'a'
            when cum_revenue <= 0.95 * total_revenue then 'b'
            else 'c'
        end as abc_class
    from monetary_ranks
),
final_enriched as (
    select
        f.*,
        a.abc_class
    from rfm_final f
    join abc a on f.customer_id = a.customer_id
)
select
    segment,
    abc_class,
    count(*) as customers,
    round(avg(monetary), 2) as avg_monetary,
    --round(avg(recency_days), 1) as avg_recency_days,
    round(avg(norm_recency), 1) as avg_norm_recency,  
    round(avg(quantity), 1) as avg_frequency
from final_enriched
group by segment, abc_class
order by avg_monetary desc;