#%%
from wikidataintegrator import wdi_core, wdi_login
logincreds = wdi_login.WDLogin(user="<wbusername>", pwd="<wbpassword>", mediawiki_api_url="<wikibase_url>/w/api.php")
