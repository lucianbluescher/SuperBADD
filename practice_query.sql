/* QUESTION: 
What is the Connectivity Status Index (CSI) for the 5 dams in Turkey 
that have the highest average discharge (dis_avg_ls)?
*/

-- Select dam names, their discharge, and their connectivity score
SELECT 
    g.dam_name,
    g.country,
    -- Aggregate
    MAX(g.dis_avg_ls) AS max_discharge_ls,
    AVG(r.csi) AS avg_connectivity_index
FROM 
    gdw g
JOIN 
    world_rivers_ffr r ON g.hyriv_id = r.hyriv_id
WHERE 
    g.country = 'Turkey'
GROUP BY 
    g.dam_name, 
    g.country
ORDER BY 
    max_discharge_ls DESC
LIMIT 5;

/*
Turkey is planning many future hydropower projects. It also already has many dams. It is important for planning to know 
what dams and what sizes are most impactful on river network connectivitvty. This query gives a glimpse at some of the biggest
dams in Turkey and helps us confirm that dams with more discharge generally have lower CSIs, meaning a larger negative impact on 
negativity, from a planning standpoint, with a little more research we could infer that cascade dams (the basis of our capstone) 
and smaller, passable dams could have less of an impact on connectivity. 
*/