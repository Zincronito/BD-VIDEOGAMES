
## Top 5 videojuegos más vendidos globalmente (suma de todas las regiones)

SELECT g.game_name, SUM(rs.num_sales) AS total_sales
FROM region_sales rs
JOIN game_platform gp ON rs.game_platform_id = gp.id
JOIN game_publisher gpub ON gp.game_publisher_id = gpub.id
JOIN game g ON gpub.game_id = g.id
GROUP BY g.game_name
ORDER BY total_sales DESC
LIMIT 5;

## Número de juegos publicados por año y plataforma
SELECT p.platform_name, gp.release_year, COUNT(*) AS num_games
FROM game_platform gp
JOIN platform p ON gp.platform_id = p.id
GROUP BY p.platform_name, gp.release_year
ORDER BY gp.release_year, p.platform_name;

## Editorial con más ventas acumuladas en todas las plataformas y regiones

SELECT pub.publisher_name, SUM(rs.num_sales) AS total_sales
FROM region_sales rs
JOIN game_platform gp ON rs.game_platform_id = gp.id
JOIN game_publisher gpub ON gp.game_publisher_id = gpub.id
JOIN publisher pub ON gpub.publisher_id = pub.id
GROUP BY pub.publisher_name
ORDER BY total_sales DESC
LIMIT 1;
