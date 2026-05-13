-- Question: For dams in Turkey, which report the highest discharge (dis_avg_ls),
-- and what is the average CSI on the FFR segment with the same hyriv_id?

SELECT
    g.dam_name,
    g.country,
    MAX(g.dis_avg_ls) AS max_discharge_ls,
    AVG(f.csi) AS avg_csi
FROM gdw AS g
JOIN ffr AS f ON g.hyriv_id = f.hyriv_id
WHERE g.country = 'Turkey'
GROUP BY g.dam_name, g.country
ORDER BY max_discharge_ls DESC NULLS LAST
LIMIT 5;
