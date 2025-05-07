import pandas as pd
from fastapi import FastAPI, HTTPException, Path
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from io import BytesIO
import os
import numpy as np  
from fastapi import Query

app = FastAPI()

# Configuración de la base de datos
MYSQL_HOST = os.getenv('MYSQL_HOST', 'mysql')
MYSQL_USER = os.getenv('MYSQL_USER', 'user')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'password')
MYSQL_DB = os.getenv('MYSQL_DB', 'video_games')

engine = create_engine(f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}')

# Carga inicial de datos
tablas = ['genre', 'game', 'game_platform', 'game_publisher', 'platform', 'publisher', 'region', 'region_sales']
carpeta_destino = '/app/data'

def extraer_tablas():
    os.makedirs(carpeta_destino, exist_ok=True)
    for tabla in tablas:
        try:
            df = pd.read_sql(f"SELECT * FROM {tabla}", con=engine)
            archivo_salida = os.path.join(carpeta_destino, f"{tabla}.csv")
            df.to_csv(archivo_salida, index=False)
            print(f"Tabla {tabla} exportada a {archivo_salida}")
        except Exception as e:
            print(f"Error al exportar {tabla}: {str(e)}")

extraer_tablas()

# Cargar datos en DataFrames
dfs = {}
for tabla in tablas:
    ruta_archivo = os.path.join(carpeta_destino, f"{tabla}.csv")
    if os.path.exists(ruta_archivo):
        dfs[tabla] = pd.read_csv(ruta_archivo)
    else:
        print(f"Archivo {tabla}.csv no encontrado")



@app.get("/top_plataformas/tabla", response_class=HTMLResponse)
def top_juegos_por_plataforma(
    plataforma: str = "psp",
    limit: int = 10
):
    """
    Muestra los juegos más vendidos para una plataforma específica
    (Versión corregida según diagrama ER)
    """
    try:
        query = """
        SELECT 
            g.game_name as "Juego",
            gp.release_year as "Año",
            ROUND(SUM(rs.num_sales), 2) as "Ventas (M)"
        FROM game g
        JOIN game_publisher gpub ON g.id = gpub.game_id
        JOIN game_platform gp ON gpub.id = gp.game_publisher_id
        JOIN platform p ON gp.platform_id = p.id
        JOIN region_sales rs ON gp.id = rs.game_platform_id
        WHERE p.platform_name LIKE %s
        GROUP BY g.game_name, gp.release_year
        ORDER BY "Ventas (M)" DESC
        LIMIT %s;
        """
        
        # Ejecutar consulta
        df = pd.read_sql(query, con=engine, params=(f"%{plataforma}%", limit))
        
        if df.empty:
            return HTMLResponse(
                content=f"""
                <html>
                    <body>
                        <h2>No se encontraron juegos para {plataforma}</h2>
                        <p>Prueba con otro nombre de plataforma como:</p>
                        <ul>
                            <li>PlayStation</li>
                            <li>Xbox</li>
                            <li>Nintendo</li>
                            <li>PC</li>
                        </ul>
                    </body>
                </html>
                """,
                status_code=404
            )
        
        # HTML mejorado
        html_content = f"""
        <html>
            <head>
                <title>Top {limit} juegos para {plataforma}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    h2 {{ color: #2c3e50; text-align: center; }}
                    table {{
                        width: 80%;
                        margin: 20px auto;
                        border-collapse: collapse;
                    }}
                    th, td {{
                        padding: 10px;
                        text-align: left;
                        border-bottom: 1px solid #ddd;
                    }}
                    th {{
                        background-color: #3498db;
                        color: white;
                    }}
                    tr:nth-child(even) {{ background-color: #f2f2f2; }}
                    tr:hover {{ background-color: #e6f7ff; }}
                </style>
            </head>
            <body>
                <h2>Top {limit} juegos para {plataforma}</h2>
                {df.to_html(index=False, classes='data-table', float_format='{:,.2f}'.format)}
            </body>
        </html>
        """
        
        return HTMLResponse(content=html_content)
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al generar tabla: {str(e)}"
        )

#endpoint de exitos por año 
@app.get("/analisis/exitos_por_año/tabla", response_class=HTMLResponse)
def exitos_por_año(year: int = 2010):
    """
    Muestra los juegos más exitosos por ventas en un año específico
    """
    try:
        query = """
        SELECT 
            g.game_name as "Juego",
            p.platform_name as "Plataforma",
            ROUND(SUM(rs.num_sales), 2) as "Ventas (M)"
        FROM game g
        JOIN game_publisher gp ON g.id = gp.game_id
        JOIN game_platform gpl ON gp.id = gpl.game_publisher_id
        JOIN platform p ON gpl.platform_id = p.id
        JOIN region_sales rs ON gpl.id = rs.game_platform_id
        WHERE gpl.release_year = %s
        GROUP BY g.game_name, p.platform_name
        ORDER BY "Ventas (M)" DESC
        LIMIT 10;
        """
        df = pd.read_sql(query, con=engine, params=(year,))
        
        if df.empty:
            return HTMLResponse(
                content=f"""
                <html>
                    <body>
                        <h2>No se encontraron juegos para el año {year}</h2>
                        <p>Prueba con otro año entre 1980 y 2020</p>
                    </body>
                </html>
                """,
                status_code=404
            )
        
        # HTML mejorado
        html_content = f"""
        <html>
            <head>
                <title>Top 10 juegos más exitosos de {year}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    h2 {{ color: #2c3e50; text-align: center; }}
                    table {{
                        width: 80%;
                        margin: 20px auto;
                        border-collapse: collapse;
                    }}
                    th, td {{
                        padding: 10px;
                        text-align: left;
                        border-bottom: 1px solid #ddd;
                    }}
                    th {{
                        background-color: #3498db;
                        color: white;
                    }}
                    tr:nth-child(even) {{ background-color: #f2f2f2; }}
                    tr:hover {{ background-color: #e6f7ff; }}
                </style>
            </head>
            <body>
                <h2>Top 10 juegos más exitosos de {year}</h2>
                {df.to_html(index=False, classes='data-table', float_format='{:,.2f}'.format)}
            </body>
        </html>
        """
        
        return HTMLResponse(content=html_content)
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al generar tabla: {str(e)}"
        )


@app.get("/tendencias/plataformas_decada/tabla", response_class=HTMLResponse)
def plataformas_decada(decada: int = 2000):
    """
    Top plataformas por ventas en una década específica
    """
    try:
        start_year = decada
        end_year = decada + 9
        
        query = """
        SELECT 
            p.platform_name as "Plataforma",
            ROUND(SUM(rs.num_sales), 2) as "Ventas Totales (M)"
        FROM platform p
        JOIN game_platform gp ON p.id = gp.platform_id
        JOIN region_sales rs ON gp.id = rs.game_platform_id
        WHERE gp.release_year BETWEEN %s AND %s
        GROUP BY p.platform_name
        ORDER BY "Ventas Totales (M)" DESC
        LIMIT 10;
        """
        df = pd.read_sql(query, con=engine, params=(start_year, end_year))
        
        if df.empty:
            return HTMLResponse(
                content=f"""
                <html>
                    <body>
                        <h2>No se encontraron datos para la década {start_year}-{end_year}</h2>
                        <p>Prueba con otra década (ej: 1990, 2000, 2010)</p>
                    </body>
                </html>
                """,
                status_code=404
            )
        
        # HTML mejorado
        html_content = f"""
        <html>
            <head>
                <title>Top 10 plataformas de {start_year}-{end_year}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    h2 {{ color: #2c3e50; text-align: center; }}
                    table {{
                        width: 80%;
                        margin: 20px auto;
                        border-collapse: collapse;
                    }}
                    th, td {{
                        padding: 10px;
                        text-align: left;
                        border-bottom: 1px solid #ddd;
                    }}
                    th {{
                        background-color: #3498db;
                        color: white;
                    }}
                    tr:nth-child(even) {{ background-color: #f2f2f2; }}
                    tr:hover {{ background-color: #e6f7ff; }}
                </style>
            </head>
            <body>
                <h2>Top 10 plataformas más populares ({start_year}-{end_year})</h2>
                {df.to_html(index=False, classes='data-table', float_format='{:,.2f}'.format)}
            </body>
        </html>
        """
        
        return HTMLResponse(content=html_content)
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al generar tabla: {str(e)}"
        )
    

@app.get("/tablas", response_class=HTMLResponse)
def menu_tablas():
    html_content = """
    <html>
        <head>
            <title>Menú de Tablas</title>
            <style>
                body { 
                    font-family: Arial, sans-serif; 
                    margin: 40px;
                    background-color: #f5f5f5;
                }
                h1 { 
                    color: #2c3e50; 
                    text-align: center;
                    margin-bottom: 30px;
                }
                .container {
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    max-width: 800px;
                    margin: 0 auto;
                }
                .card {
                    background: white;
                    border-radius: 8px;
                    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                    padding: 20px;
                    margin-bottom: 20px;
                    width: 100%;
                }
                .card h2 {
                    color: #3498db;
                    margin-top: 0;
                }
                .card p {
                    color: #7f8c8d;
                }
                a.btn {
                    display: inline-block;
                    padding: 10px 20px;
                    background-color: #3498db;
                    color: white;
                    text-decoration: none;
                    border-radius: 5px;
                    font-weight: bold;
                    transition: background-color 0.3s;
                }
                a.btn:hover {
                    background-color: #2980b9;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Tablas Disponibles</h1>
                
                <div class="card">
                    <h2>Top Plataformas por Década</h2>
                    <p>Muestra las plataformas más populares según ventas en una década específica</p>
                    <a href="/tendencias/plataformas_decada/tabla?decada=2000" class="btn">Ver Tabla</a>
                </div>
                
                <div class="card">
                    <h2>Éxitos por Año</h2>
                    <p>Lista los juegos más exitosos por ventas en un año específico</p>
                    <a href="/analisis/exitos_por_año/tabla?year=2010" class="btn">Ver Tabla</a>
                </div>
                
                <div class="card">
                    <h2>Top Juegos por Plataforma</h2>
                    <p>Muestra los juegos más vendidos para una plataforma específica</p>
                    <a href="/top_plataformas/tabla?plataforma=psp" class="btn">Ver Tabla</a>
                </div>
            </div>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content)




# 2. Endpoints de Comparativas
@app.get("/comparar/editoras/grafico")
def comparar_editoras(
    publisher1: str = Query(..., description="Nombre exacto de la primera editora"),
    publisher2: str = Query(..., description="Nombre exacto de la segunda editora"),
    region: str = Query("japan", description="Nombre de la región a comparar")
):
    """
    Compara ventas de dos editoras en una región específica
    
    Args:
        publisher1: Nombre exacto de la primera editora (ej: 'Nintendo')
        publisher2: Nombre exacto de la segunda editora (ej: 'Sony Computer Entertainment')
        region: Nombre de la región (ej: 'japan', 'europe')
    """
    try:
        # Consulta más precisa sin wildcards
        query = """
        SELECT 
            pub.publisher_name, 
            COALESCE(SUM(rs.num_sales), 0) as total_sales
        FROM publisher pub
        LEFT JOIN game_publisher gp ON pub.id = gp.publisher_id
        LEFT JOIN game_platform gpl ON gp.id = gpl.game_publisher_id
        LEFT JOIN region_sales rs ON gpl.id = rs.game_platform_id
        LEFT JOIN region r ON rs.region_id = r.id AND r.region_name = %s
        WHERE pub.publisher_name IN (%s, %s)
        GROUP BY pub.publisher_name
        ORDER BY 
            CASE pub.publisher_name
                WHEN %s THEN 1
                WHEN %s THEN 2
                ELSE 3
            END;
        """
        
        # Ejecutar con parámetros en orden correcto
        df = pd.read_sql(query, con=engine, 
                        params=(region, publisher1, publisher2, publisher1, publisher2))
        
        # Verificar que tengamos datos para ambas editoras
        publishers_in_results = set(df['publisher_name'])
        if publisher1 not in publishers_in_results:
            df = pd.concat([df, pd.DataFrame({
                'publisher_name': [publisher1],
                'total_sales': [0]
            })])
        
        if publisher2 not in publishers_in_results:
            df = pd.concat([df, pd.DataFrame({
                'publisher_name': [publisher2],
                'total_sales': [0]
            })])
        
        # Ordenar según el orden de los parámetros
        df['order'] = df['publisher_name'].apply(
            lambda x: 1 if x == publisher1 else 2
        )
        df = df.sort_values('order').drop('order', axis=1)
        
        # Crear gráfico con colores consistentes
        fig, ax = plt.subplots(figsize=(10, 6))
        colors = ['#3498db', '#e74c3c']  # Azul para publisher1, Rojo para publisher2
        
        bars = ax.bar(
            x=df['publisher_name'],
            height=df['total_sales'],
            color=colors,
            alpha=0.8
        )
        
        # Personalización del gráfico
        ax.set_title(f"Comparativa de ventas en {region.capitalize()}", pad=20)
        ax.set_ylabel("Ventas totales (millones)", labelpad=10)
        ax.set_xlabel("Editora", labelpad=10)
        
        # Añadir valores en las barras
        for bar in bars:
            height = bar.get_height()
            ax.text(
                bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f}M',
                ha='center', va='bottom',
                fontsize=10
            )
        
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Generar imagen
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        plt.close()
        buf.seek(0)
        
        return StreamingResponse(buf, media_type="image/png")
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al generar gráfico comparativo: {str(e)}"
        )



# 4. Endpoints de Análisis Geográfico
@app.get("/geografia/distribucion_ventas/grafico")
def distribucion_ventas_juego(game_name: str = "Mario"):
    """
    Distribución regional de ventas para un juego específico
    """
    try:
        query = """
        SELECT r.region_name, SUM(rs.num_sales) as total_sales
        FROM game g
        JOIN game_publisher gp ON g.id = gp.game_id
        JOIN game_platform gpl ON gp.id = gpl.game_publisher_id
        JOIN region_sales rs ON gpl.id = rs.game_platform_id
        JOIN region r ON rs.region_id = r.id
        WHERE g.game_name LIKE %s
        GROUP BY r.region_name
        """
        df = pd.read_sql(query, con=engine, params=(f"%{game_name}%",))
        
        fig, ax = plt.subplots(figsize=(8, 8))
        df.plot(x='region_name', y='total_sales', kind='pie', 
               autopct='%1.1f%%', ax=ax, labels=df['region_name'])
        ax.set_title(f"Distribución de ventas para {game_name}")
        ax.set_ylabel("")
        
        buf = BytesIO()
        plt.savefig(buf, format="png")
        buf.seek(0)
        plt.close()
        return StreamingResponse(buf, media_type="image/png")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    


@app.get("/geografia/comparativa_juegos/grafico")
def comparativa_ventas_regiones(game1: str = "Mario", game2: str = "Zelda"):
    """
    Compara la distribución regional de ventas entre dos juegos
    Genera un gráfico de barras agrupadas por región
    """
    try:
        # Consulta para ambos juegos
        query = """
        SELECT 
            r.region_name,
            SUM(CASE WHEN g.game_name LIKE %s THEN rs.num_sales ELSE 0 END) as ventas_juego1,
            SUM(CASE WHEN g.game_name LIKE %s THEN rs.num_sales ELSE 0 END) as ventas_juego2
        FROM game g
        JOIN game_publisher gp ON g.id = gp.game_id
        JOIN game_platform gpl ON gp.id = gpl.game_publisher_id
        JOIN region_sales rs ON gpl.id = rs.game_platform_id
        JOIN region r ON rs.region_id = r.id
        WHERE g.game_name LIKE %s OR g.game_name LIKE %s
        GROUP BY r.region_name
        HAVING ventas_juego1 > 0 OR ventas_juego2 > 0
        ORDER BY r.region_name;
        """
        
        params = (f"%{game1}%", f"%{game2}%", f"%{game1}%", f"%{game2}%")
        df = pd.read_sql(query, con=engine, params=params)
        
        if df.empty:
            return Response(
                content="No se encontraron datos para los juegos especificados",
                media_type="text/plain"
            )
        
        # Configurar gráfico de barras agrupadas
        plt.figure(figsize=(12, 7))
        
        # Posiciones de las barras
        x = np.arange(len(df['region_name']))
        width = 0.35  # Ancho de las barras
        
        # Crear barras
        bars1 = plt.bar(x - width/2, df['ventas_juego1'], width, 
                       label=game1, color='#3498db', alpha=0.8)
        bars2 = plt.bar(x + width/2, df['ventas_juego2'], width, 
                       label=game2, color='#e74c3c', alpha=0.8)
        
        # Personalización
        plt.title(f"Comparativa de ventas: {game1} vs {game2} por región", pad=20)
        plt.xlabel("Región", labelpad=10)
        plt.ylabel("Ventas (millones)", labelpad=10)
        plt.xticks(x, df['region_name'])
        plt.legend()
        
        # Añadir valores en las barras
        def autolabel(bars):
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2., height,
                        f'{height:.2f}',
                        ha='center', va='bottom', fontsize=8)
        
        autolabel(bars1)
        autolabel(bars2)
        
        plt.grid(True, axis='y', linestyle='--', alpha=0.4)
        plt.tight_layout()
        
        # Generar imagen
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=100)
        plt.close()
        buf.seek(0)
        
        return StreamingResponse(buf, media_type="image/png")
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al generar gráfico comparativo: {str(e)}"
        )
    

@app.get("/publishers", response_class=HTMLResponse)
def listar_publishers(
    nombre: str = Query(None, description="Filtrar por nombre (búsqueda parcial)"),
    ventas_minimas: float = Query(None, description="Ventas mínimas en millones"),
    limit: int = Query(10, description="Límite de resultados"),
    formato: str = Query('html', description="Formato de respuesta (html/json)")
):
    """
    Lista todos los publishers con opciones de filtrado
    
    Parámetros:
    - nombre: Filtrar por nombre (búsqueda parcial)
    - ventas_minimas: Filtrar por ventas mínimas (en millones)
    - limit: Número máximo de resultados (default: 10)
    - formato: Formato de respuesta (html/json)
    """
    try:
        # Construir consulta base
        query = """
        SELECT 
            p.publisher_name AS "Editora",
            COUNT(DISTINCT g.id) AS "Juegos Publicados",
            ROUND(SUM(rs.num_sales), 2) AS "Ventas Totales (M)",
            COUNT(DISTINCT pl.id) AS "Plataformas"
        FROM publisher p
        LEFT JOIN game_publisher gp ON p.id = gp.publisher_id
        LEFT JOIN game g ON gp.game_id = g.id
        LEFT JOIN game_platform gpl ON gp.id = gpl.game_publisher_id
        LEFT JOIN platform pl ON gpl.platform_id = pl.id
        LEFT JOIN region_sales rs ON gpl.id = rs.game_platform_id
        """
        
        # Añadir condiciones WHERE según parámetros
        conditions = []
        params = []
        
        if nombre:
            conditions.append("p.publisher_name LIKE %s")
            params.append(f"%{nombre}%")
        
        if ventas_minimas is not None:
            conditions.append("SUM(rs.num_sales) >= %s")
            params.append(ventas_minimas)
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        # Añadir GROUP BY y ORDER
        query += """
        GROUP BY p.id, p.publisher_name
        HAVING "Ventas Totales (M)" IS NOT NULL
        ORDER BY "Ventas Totales (M)" DESC
        LIMIT %s;
        """
        params.append(limit)
        
        # Ejecutar consulta
        df = pd.read_sql(query, con=engine, params=params)
        
        if df.empty:
            raise HTTPException(
                status_code=404,
                detail="No se encontraron publishers con los criterios especificados"
            )
        
        # Formatear respuesta según el formato solicitado
        if formato == 'json':
            return df.to_dict(orient='records')
            
        # HTML por defecto
        html_content = f"""
        <html>
            <head>
                <title>Listado de Publishers</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    h2 {{ 
                        color: #2c3e50; 
                        text-align: center;
                        margin-bottom: 30px;
                    }}
                    .filtros {{
                        background: #f8f9fa;
                        padding: 15px;
                        border-radius: 5px;
                        margin-bottom: 20px;
                    }}
                    table {{
                        width: 90%;
                        margin: 20px auto;
                        border-collapse: collapse;
                    }}
                    th, td {{
                        padding: 12px;
                        text-align: left;
                        border-bottom: 1px solid #ddd;
                    }}
                    th {{
                        background-color: #3498db;
                        color: white;
                        position: sticky;
                        top: 0;
                    }}
                    tr:nth-child(even) {{ background-color: #f2f2f2; }}
                    tr:hover {{ background-color: #e6f7ff; }}
                    .total-row {{
                        font-weight: bold;
                        background-color: #d4edda !important;
                    }}
                </style>
            </head>
            <body>
                <h2>Listado de Publishers</h2>
                
                <div class="filtros">
                    <strong>Filtros aplicados:</strong>
                    {f"<div>Nombre contiene: '{nombre}'</div>" if nombre else ""}
                    {f"<div>Ventas mínimas: {ventas_minimas}M</div>" if ventas_minimas is not None else ""}
                </div>
                
                {df.to_html(index=False, classes='data-table', float_format='{:,.2f}'.format)}
                
                <div style="text-align: center; margin-top: 20px;">
                    <a href="/publishers?formato=json" style="color: #3498db;">Ver en formato JSON</a>
                </div>
            </body>
        </html>
        """
        
        return HTMLResponse(content=html_content)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error al obtener publishers: {str(e)}"
        )






