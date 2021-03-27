# Rebecca Lewis
# DSC 540 Final Project
# November 16, 2019
# Read in data from the cocktailsdb.com API, clean, transform and load into a database
#

import requests
import dataset
import pandas as pd
from collections import OrderedDict

def retrieve_data(url):
    request = requests.get(url)
    content = request.json()

    return content

def main():
    #connect to the database and create a table
    db = dataset.connect('sqlite:///vodka_cocktails.db')
    drink_table = db['drink_recipes']

    #before inserting any data, delete the data currently loaded
    drink_table.delete

    #Get information on drinks with vodka; the type of liquor could easily be replaced by a variable if this were an
    #application that provided recipes requested by a user
    cocktail_list = retrieve_data('https://www.thecocktaildb.com/api/json/v1/1/filter.php?i=Vodka')
    drinks_df = pd.DataFrame(cocktail_list['drinks'])

    ingredients_df = pd.DataFrame()

    #look up ingredients for each drink returned above using the drink id
    #write the results to the ingredients dataframe
    for item in cocktail_list['drinks']:
        ingredient_list = retrieve_data('https://www.thecocktaildb.com/api/json/v1/1/lookup.php?i={}'.format(item['idDrink']))
        ingredients_df = ingredients_df.append(ingredient_list['drinks'])

    #remove hyphens in column names
    ingredients_df.columns = ingredients_df.columns.str.replace('-', '')

    # don't need all the columns in the ingredients table and need to reorder them
    columnTitles = ['idDrink', 'strIBA', 'strAlcoholic', 'strGlass', 'strCategory', 'strInstructions', 'strMeasure1',
                    'strIngredient1', 'strMeasure2', 'strIngredient2', 'strMeasure3', 'strIngredient3', 'strMeasure4', 'strIngredient4',
                    'strMeasure5', 'strIngredient5', 'strMeasure6', 'strIngredient6', 'strMeasure7', 'strIngredient7', 'strMeasure8',
                    'strIngredient8','strMeasure9', 'strIngredient9','strMeasure10', 'strIngredient10','strMeasure11', 'strIngredient11',
                    'strMeasure12', 'strIngredient12', 'strMeasure13', 'strIngredient13','strMeasure14', 'strIngredient14','strMeasure15',
                    'strIngredient15']

    ingredients_df = ingredients_df[columnTitles]

    #join the drinks data with the ingredients data
    combined_df = drinks_df.set_index('idDrink').join(ingredients_df.set_index('idDrink'))

    #load the data into the sqlite database
    columnNames = combined_df.columns
    for index, row in combined_df.iterrows():
        #an the data is read into an ordered dictionary before insert to maintain column order
        newDict = OrderedDict()
        for key in columnNames:
            newDict[key] = row[key]
        drink_table.insert(newDict)


if __name__ == '__main__':
    main()


