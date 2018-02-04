from csv_generator import buildReactionsCSVs

AppID = '264737207353432'
AppSecret = '460c5a58dd6ddd6997b2645b1ad37cdd'

dicCandidates = \
    {
        'Carlos Alvarado'           : '171990063299676',
        'Antonio Alvarez Desanti'   : '117702158266385',
        'Rodolfo Piza'              : '605074109521470',
        'Juan Diego Castro'         : '105896236141807',
        'Otto Guevara'              : '117011658390752',
        'Edgardo Araya'             : '635661853130707',
        'Rodolfo Hernandez'         : '303450469784702',
        'Fabricio Alvarado'         : '167533350116993'
    }

for candidateName, candidateID in dicCandidates.items():
    buildReactionsCSVs(AppID, AppSecret, dicCandidates[candidateName], '1509494400', '1517598000',
                       candidateName + '_Nodes', candidateName + '_Edges', version="2.10")