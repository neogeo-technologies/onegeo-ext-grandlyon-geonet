![Alt text](grandlyon_320x66.png)

# Extension de profil de recherche Onegeo __{geonet}__.py

Service de recherche __data.grandlyon.com__ (expérimental).

## Configuration du service

### URL du service

[http://localhost/onegeo/api/profiles/__{geonet}__/search?](
    http://localhost/onegeo/api/profiles/geonet/search)

### Paramètres de chaîne de recherche

| Paramètre    | Type    | Description                                         |
| ------------ | ------- | --------------------------------------------------- |
| any          | string  | Texte à rechercher                                  |
| fast         | boolean | Activer le mode 'fast'                              |
| from         | integer | Index du premier document retourné                  |
| to           | integer | Index du dernier document retourné                  |
| type         | string  | Filtrer sur le type de ressource                    |


### Format des résultats

Le service retourne les résultats dans un document XML.
Celui-ci est structuré de la façon suivante :

``` XML
<response from="0" to="4">
    <summary count="5">
        <!-- Contient les statistiques de résultat -->
    </summary>
    <metadata><!-- Premier résultat  --></metadata>
    <metadata><!-- Deuxième résultat --></metadata>
    <metadata><!-- Troisième résultat --></metadata>
    <metadata><!-- Quatrième résultat --></metadata>
    <metadata><!-- Cinquième résultat --></metadata>
    </summary>
</response>
```
...
