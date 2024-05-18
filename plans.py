import pandas as pd

def plan():
    print("merging saeplus and wisphub dataframes")
    saeplus = pd.read_excel("saeplus.xlsx")

    saeplus['unique_plan'] = saeplus.groupby(['Det. Susc. Int.', 'Suscripción']).ngroup()
    saeplus['new_plan'] = saeplus['Det. Susc. Int.'] + '_' + (saeplus['unique_plan'] + 1).astype(str)

    saeplus = saeplus.sort_values(by=['Det. Susc. Int.', 'Suscripción'])
    saeplus = saeplus[["subscriber","Estatus",'Det. Susc. Int.', 'Suscripción', 'new_plan']]
    saeplus.to_excel("plans.xlsx", index=False)
    print("plans successfully")

plan()