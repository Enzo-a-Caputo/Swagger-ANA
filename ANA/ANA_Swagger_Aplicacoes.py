import geopandas as gpd
import matplotlib.pyplot as plt
import contextily as ctx  

class Aplicacoes:
    def __init__(self):
        pass


    def achar_estacoes_pela_bacia(self, shapefile_bacia_path, shapefile_ANA_path = 'Estações_Fluviométricas_e_Pluviométricas_da_Rede.shp', shapefile_rede_drenagem = None):
        """
        Encontra as estações hidrológicas pertencentes à bacia especificada.

        Parâmetros:
            shapefile_bacia_path (str): path do shape da bacia hidrográfica.

        Retorna:
            cod_est_P (list): Lista de códigos das estações pluviométricas.
            cod_est_Q (list): Lista de códigos das estações fluviométricas.
            fig (matplotlib.figure.Figure): Figura gerada, se shapefile_rede_drenagem não for None.
        """
        
        gdf = gpd.read_file(shapefile_ANA_path)  ## Importando o arquivo de estações telemétricas
                                                 ## Shape com todas as estações PLU e FLU da ANA, o ideal é que já tenhamos um pronto para rodar automaticamente
        clip_geometry = gpd.read_file(shapefile_bacia_path)  ## Bacia hidrográfica de interesse. Usaremos para selecionar as estações em seu interior
        
        gdf.to_crs(epsg=4326, inplace=True)  ## Mudando o Sistemas de Coordenadas para WGS84 para ficar igual ao da bacia.
        
        clipped_gdf = gpd.clip(gdf, clip_geometry)  ## Cortando as estações usando a bacia como referência
        
        cod_est_P = clipped_gdf[clipped_gdf['TIPOESTACA'] == 'Pluviométrica']['CODIGO'].tolist()  ## Selecionando apenas o código da estação plu e salvando em um objeto tipo lista
        cod_est_Q = clipped_gdf[clipped_gdf['TIPOESTACA'] == 'Fluviométrica']['CODIGO'].tolist()  ## Selecionando apenas o código da estação flu e salvando em um objeto tipo lista
        
        fig = None  
        
        if shapefile_rede_drenagem is not None:
            dren = gpd.read_file(shapefile_rede_drenagem)  
            
            fig, ax = plt.subplots(figsize=(10, 10))  
            
            clip_geometry.plot(ax=ax, edgecolor='black', facecolor='None', linewidth=1.5, label='Bacia')  ## Plotando bacia
            dren.plot(ax=ax, color='blue', edgecolor='None', alpha=0.8, label='Rede_drenagem')  ## Plotando rede de drenagem
            clipped_gdf.plot(ax=ax, color='red', edgecolor='k', alpha=1.0, label='Estações')  ## Plotando estações selecionadas
            
            ax.legend()  ## Usando ax.legend() em vez de plt.legend() para garantir que a legenda seja referenciada corretamente
            
            plt.title('Multiple Geospatial Files')
            plt.xlabel('Longitude')
            plt.ylabel('Latitude')
        
        return cod_est_P, cod_est_Q, fig

    def achar_estacoes_pela_bacia_2(self, shapefile_bacia_path, shapefile_ANA_path='Estações_Fluviométricas_e_Pluviométricas_da_Rede.shp', shapefile_rede_drenagem=None):
        """
        Encontra as estações hidrológicas pertencentes à bacia especificada.

        Parâmetros:
            shapefile_bacia_path (str): path do shape da bacia hidrográfica.
            shapefile_ANA_path (str): path do shapefile das estações da ANA.
            shapefile_rede_drenagem (str): path do shapefile da rede de drenagem (opcional).

        Retorna:
            cod_est_P (list): Lista de códigos das estações pluviométricas.
            cod_est_Q (list): Lista de códigos das estações fluviométricas.
            fig (matplotlib.figure.Figure): Figura gerada.
        """
        
        gdf = gpd.read_file(shapefile_ANA_path)  # Estações da ANA
        clip_geometry = gpd.read_file(shapefile_bacia_path)  # Bacia hidrográfica
        
        # Garantindo que ambos estão no mesmo CRS
        clip_geometry = clip_geometry.to_crs(epsg=4326)
        gdf = gdf.to_crs(epsg=4326)
        
        clipped_gdf = gpd.clip(gdf, clip_geometry)
        
        # Selecionando os códigos das estações
        cod_est_P = clipped_gdf[clipped_gdf['Tipo_Estac'] == 'Pluviometrica']['codigoesta'].tolist()
        cod_est_Q = clipped_gdf[clipped_gdf['Tipo_Estac'] == 'Fluviometrica']['codigoesta'].tolist()
        
        
        fig, ax = plt.subplots(figsize=(10, 10))
        
        clip_geometry.plot(ax=ax, edgecolor='black', facecolor='None', linewidth=1.5, label='Bacia')
        
        # Processando e plotando a rede de drenagem se for fornecida
        if shapefile_rede_drenagem is not None and shapefile_rede_drenagem != '':
            dren = gpd.read_file(shapefile_rede_drenagem)
            dren = dren.to_crs(epsg=4326)
            dren_clipped = gpd.clip(dren, clip_geometry)
            
            dren_clipped.plot(ax=ax, color='blue', linewidth=0.8, alpha=0.8, label='Rede de drenagem')
        
        # Plotando as estações diferenciadas por tipo
        if len(cod_est_P) > 0:
            pluv = clipped_gdf[clipped_gdf['Tipo_Estac'] == 'Pluviometrica']
            pluv.plot(ax=ax, color='green', marker='^', markersize=50, edgecolor='k', alpha=1.0, label='Estações Pluviométricas')
        
        if len(cod_est_Q) > 0:
            fluv = clipped_gdf[clipped_gdf['Tipo_Estac'] == 'Fluviometrica']
            fluv.plot(ax=ax, color='red', marker='o', markersize=50, edgecolor='k', alpha=1.0, label='Estações Fluviométricas')
        
        ax.legend()
        plt.title('Estações na Bacia Hidrográfica')
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.tight_layout()
        
        return cod_est_P, cod_est_Q, fig, ax
    



    def achar_estacoes_pela_bacia_3(
        shapefile_bacia_path,
        shapefile_ANA_path='Estações_Fluviométricas_e_Pluviométricas_da_Rede.shp',
        shapefile_rede_drenagem=None
    ):
        """
        Encontra as estações hidrológicas pertencentes à bacia especificada e plota sobre fundo de mapa (Google/OSM).

        Parâmetros:
            shapefile_bacia_path (str): path do shape da bacia hidrográfica.
            shapefile_ANA_path (str): path do shapefile das estações da ANA.
            shapefile_rede_drenagem (str): path do shapefile da rede de drenagem (opcional).

        Retorna:
            cod_est_P (list): Lista de códigos das estações pluviométricas.
            cod_est_Q (list): Lista de códigos das estações fluviométricas.
            fig, ax: Figura e eixos matplotlib com o mapa.
        """
        
        # Importando shapefiles
        gdf = gpd.read_file(shapefile_ANA_path)
        clip_geometry = gpd.read_file(shapefile_bacia_path)

        # Garantindo CRS consistente
        gdf = gdf.to_crs(epsg=4326)
        clip_geometry = clip_geometry.to_crs(epsg=4326)

        # Clipping estações pela bacia
        clipped_gdf = gpd.clip(gdf, clip_geometry)

        # Selecionando os códigos
        cod_est_P = clipped_gdf[clipped_gdf['TIPOESTACA'] == 'Pluviométrica']['CODIGO'].tolist()
        cod_est_Q = clipped_gdf[clipped_gdf['TIPOESTACA'] == 'Fluviométrica']['CODIGO'].tolist()

        # Reprojetando tudo para EPSG:3857 (necessário para contextily)
        clip_geometry = clip_geometry.to_crs(epsg=3857)
        clipped_gdf = clipped_gdf.to_crs(epsg=3857)

        if shapefile_rede_drenagem:
            dren = gpd.read_file(shapefile_rede_drenagem).to_crs(epsg=4326)
            dren = gpd.clip(dren, clip_geometry.to_crs(epsg=4326))
            dren = dren.to_crs(epsg=3857)
        else:
            dren = None

        # Criando a figura
        fig, ax = plt.subplots(figsize=(10, 10))

        # Plotando bacia
        clip_geometry.plot(ax=ax, edgecolor='black', facecolor='None', linewidth=1.5, label='Bacia')

        # Rede de drenagem
        if dren is not None:
            dren.plot(ax=ax, color='blue', linewidth=0.8, alpha=0.8, label='Rede de drenagem')

        # Estações diferenciadas
        if len(cod_est_P) > 0:
            pluv = clipped_gdf[clipped_gdf['TIPOESTACA'] == 'Pluviométrica']
            pluv.plot(ax=ax, color='green', marker='^', markersize=50, edgecolor='k', label='Pluviométricas')

        if len(cod_est_Q) > 0:
            fluv = clipped_gdf[clipped_gdf['TIPOESTACA'] == 'Fluviométrica']
            fluv.plot(ax=ax, color='red', marker='o', markersize=50, edgecolor='k', label='Fluviométricas')

        # Adicionando mapa de fundo (tiles do Google/OSM)
        ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, zoom=12)

        # Ajustes
        ax.legend()
        plt.title('Estações na Bacia Hidrográfica (com mapa de fundo)')
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.tight_layout()

        return cod_est_P, cod_est_Q, fig, ax
