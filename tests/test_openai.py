import unittest
import logging
import json
from src.openaiQuery import OpenAIQuery

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s: %(message)s")


class TestOpenAIAPI(unittest.TestCase):
    def test_pretty_print(self):
        with open("resources/prompts/generic_v4_example_output.txt") as f:
            gpt_output = json.load(f)
        OpenAIQuery.pretty_print_rels(gpt_output)


    def test_simple_query(self):
        text = ("DuPont (formerly Pont Ltd.)) owned and operated the PLW Facility as a manufacturing facility for almost a century, "
                "producing lead azide, aluminum and bronze shelled blasting caps, metal wires, and aluminum and copper "
                "shells at the site. Pompton Lake, the Ramapo River, the Wanaque River and Acid Brook are all on or "
                "adjacent to the property, which is also adjacent to the Ramapo Mountain State Forest, as well as "
                "residential communities.")
        text2 = """Minnesota        Pollution     Control      Agency

                                                    RECEIVED
                                                              1983
                                                     MAR1G
 March  15,                                                       FM
            1983                                     ENVIRONMENT
                                                       POLLUTIO:
                                                     &

Mr.  Fred Robinette
Chemolite  Utilities
3M
Building  22 Chemolite
Box  33131
St.  Paul, Minnesota  55133

Dear Mr.  Robinette:

Re:
      Inspection of the 3M Chemolite Incinerator (SW-9) and Boiler Ash
     Disposal  Site (SW-224)
This
     letter will  acknowledge that the Minnesota Pollution Control Agency (MPCA)
inspected the  above mentioned facilities on March 3, 1983.  The purpose of
the inspections was  to determine facility compliance with their respective
permits.  During  the inspections I observed a permit violation and other con
cerns that the MPCA  is requesting 3M to address.
Concerns:

1.  At the time of my  inspection I observed the "stock piling" of hazardous
    wastes  in the form of incinerator ash, sewage sludge, and unburned resins.
    Other wastes present  included brick and related waste from the October
    1982 rebricking of the  kiln.  By storing these wastes on top of the boiler
    ash disposal site,  3M has violated provisions of their SW-224 permit.
    The permit approves the deposit  of boiler ash only and does not contain
    provisions for the storage of  hazardous and other solid wastes.

2.
    Since the incinerator ash, and  possibly the unburned resins and sewage sludges
    are considered hazardous under  the federal regulations (40 CFR Part 261)
    and are currently stored  in piles, 3M will be required to pursue one of
    the following options"""

        text3 = """Perfluorinated chemicals (PFASs) stem from a wide range of sources and have been detected in
aquatic ecosystems worldwide, including the upper Midwest and the state of Minnesota in the USA.
This study investigated whether fish with high body burden levels of PFASs in the Twin Cities Metro
Areas showed any evidence of adverse effects at the level of the transcriptome. We hypothesized
that fish with higher body burden levels of PFASs would exhibit molecular responses in the liver and
testis that were suggestive of oxidative and general stress, as well as impaired reproduction.
Concentrations of PFASs in largemouth bass varied significantly across the sampled lakes, with the
lowest concentrations of PFASs found in fish from Steiger and Upper Prior Lakes and the highest
concentrations found in fish from Calhoun and Twin Lakes. Largemouth bass with high PFAS
concentrations exhibited changes in the expression of genes related to lipid metabolism, energy
production, RNA processing, protein production/degradation and contaminant detoxification, all of
which are consistent with biomarker responses observed in other studies with PFASs. However,
given the wide range of genes that were differentially expressed across the lakes and the variability
observed in the mechanisms through which biological processes were affected, it is unlikely that
PFASs are the only stressors affecting largemouth bass in the Twin Cities Metro Areas lakes. Indeed,
Twin Lake is affected by the Joslyn superfund site which contains polycyclic aromatic hydrocarbons,
pentachlorophenol, polychlorinated biphenyls, and dioxins. These compounds are also expected to
drive the transcriptomics responses observed, but to what degree is difficult to ascertain at this time."""

        with open("resources/gpt_output__text.txt", 'r') as f:
            text = f.read()

        openai = OpenAIQuery("resources/prompts", "generic_v4", 4000)
        result = openai.query(text3, "gpt-4")
        self.assertTrue('entities' in result, "Output does not contain entities.")
        self.assertTrue('relations' in result, "Output does not contain relations.")


if __name__ == '__main__':
    unittest.main()
