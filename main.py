import colorama
from colorama import Fore, Style
import logging
from create_dataset import *
from import_into_sf import *
from refactor_municipalities import *
from refactor_zipcodes import *

TABLE_NAME = 'Asset'

# define menu functions
def option_one():
    zipfilename = '202304_Tourism_Insights_Basilicata_v3.zip'
    print()
    print(Fore.LIGHTGREEN_EX + 'Creazione Dataset.' + Style.BRIGHT)
    create_dataset(zipfilename=zipfilename)
    print()

def option_two():
    print()
    print(Fore.LIGHTYELLOW_EX + 'Import into Salesforce.' + Style.BRIGHT)
    import_into_salesforce(TABLE_NAME,'data/all_datanew_2023-04-24_113522.csv',is_test=True,can_delete=True, is_sdo=False)
    print()

def option_three():
    print()
    print(Fore.LIGHTBLUE_EX + 'Refactor Zip Codes into Salesforce.' + Style.BRIGHT)
    refactor_zipcode(TABLE_NAME, is_test=True, is_sdo=False)
    print()

def option_four():
    print()
    print(Fore.LIGHTMAGENTA_EX + 'Refactor Municipalities into Salesforce.' + Style.BRIGHT)
    refactor_municipality(TABLE_NAME, is_test=True, is_sdo=False)
    print()

def exit_program():
    print()
    print(Fore.LIGHTRED_EX + 'Exiting program.' + Style.BRIGHT)
    logging.info('Exiting program.')
    print()
    exit()

def main():
    # initialize the logger
    logging.basicConfig(filename='logs/menu.log', level=logging.INFO)

    # initialize colorama
    colorama.init()

    # define menu options
    menu_options = [
        {'name': 'Create Dataset', 'function': option_one},
        {'name': 'Import into Salesforce', 'function': option_two},
        {'name': 'Refactoring Zip Codes', 'function': option_three},
        {'name': 'Refactoring Municipalities', 'function': option_four},
        {'name': 'Exit', 'function': exit_program}
    ]


    # display the menu
    while True:
        print(Fore.LIGHTCYAN_EX + '*'*50 +'Tourism Insight Salesforce Custom Dataloader' + '*'*50 + Style.BRIGHT)
        for i, option in enumerate(menu_options):
            print(f'{i+1}. {option["name"]}')
        print()
        selection = input('Please select an option: ')
        try:
            selection = int(selection)
            if selection < 1 or selection > len(menu_options):
                raise ValueError()
            menu_options[selection-1]['function']()
        except ValueError:
            print(Fore.LIGHTRED_EX + 'Invalid selection. Please try again.' + Style.BRIGHT)
            logging.error(f'Invalid selection: {selection}')
        except Exception as e:
            print(Fore.LIGHTRED_EX + 'An error occurred. Please try again.' + Style.BRIGHT)
            logging.exception(e)

if __name__ == '__main__':
    main()