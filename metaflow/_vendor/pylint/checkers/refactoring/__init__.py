# Copyright (c) 2016-2020 Claudiu Popa <pcmanticore@gmail.com>
# Copyright (c) 2016-2017 Łukasz Rogalski <rogalski.91@gmail.com>
# Copyright (c) 2016 Moises Lopez <moylop260@vauxoo.com>
# Copyright (c) 2016 Alexander Todorov <atodorov@otb.bg>
# Copyright (c) 2017-2018, 2020 hippo91 <guillaume.peillex@gmail.com>
# Copyright (c) 2017-2018 Ville Skyttä <ville.skytta@iki.fi>
# Copyright (c) 2017-2018 Bryce Guinta <bryce.paul.guinta@gmail.com>
# Copyright (c) 2017 Hugo <hugovk@users.noreply.github.com>
# Copyright (c) 2017 Łukasz Sznuk <ls@rdprojekt.pl>
# Copyright (c) 2017 Alex Hearn <alex.d.hearn@gmail.com>
# Copyright (c) 2017 Antonio Ossa <aaossa@uc.cl>
# Copyright (c) 2018-2019 Sushobhit <31987769+sushobhit27@users.noreply.github.com>
# Copyright (c) 2018 Justin Li <justinnhli@gmail.com>
# Copyright (c) 2018 Jim Robertson <jrobertson98atx@gmail.com>
# Copyright (c) 2018 Lucas Cimon <lucas.cimon@gmail.com>
# Copyright (c) 2018 Ben James <benjames1999@hotmail.co.uk>
# Copyright (c) 2018 Tomer Chachamu <tomer.chachamu@gmail.com>
# Copyright (c) 2018 Nick Drozd <nicholasdrozd@gmail.com>
# Copyright (c) 2018 Konstantin Manna <Konstantin@Manna.uno>
# Copyright (c) 2018 Konstantin <Github@pheanex.de>
# Copyright (c) 2018 Matej Marušák <marusak.matej@gmail.com>
# Copyright (c) 2018 Mr. Senko <atodorov@mrsenko.com>
# Copyright (c) 2019-2021 Pierre Sassoulas <pierre.sassoulas@gmail.com>
# Copyright (c) 2019 Rémi Cardona <remi.cardona@polyconseil.fr>
# Copyright (c) 2019 Robert Schweizer <robert_schweizer@gmx.de>
# Copyright (c) 2019 PHeanEX <github@pheanex.de>
# Copyright (c) 2019 Paul Renvoise <PaulRenvoise@users.noreply.github.com>
# Copyright (c) 2020 ethan-leba <ethanleba5@gmail.com>
# Copyright (c) 2020 lrjball <50599110+lrjball@users.noreply.github.com>
# Copyright (c) 2020 Yang Yang <y4n9squared@gmail.com>
# Copyright (c) 2020 Anthony Sottile <asottile@umich.edu>
# Copyright (c) 2021 Jaehoon Hwang <jaehoonhwang@users.noreply.github.com>
# Copyright (c) 2021 Marc Mueller <30130371+cdce8p@users.noreply.github.com>

# Licensed under the GPL: https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
# For details: https://github.com/PyCQA/pylint/blob/main/LICENSE

"""Looks for code which can be refactored."""


from metaflow._vendor.pylint.checkers.refactoring.implicit_booleaness_checker import (
    ImplicitBooleanessChecker,
)
from metaflow._vendor.pylint.checkers.refactoring.not_checker import NotChecker
from metaflow._vendor.pylint.checkers.refactoring.recommendation_checker import RecommendationChecker
from metaflow._vendor.pylint.checkers.refactoring.refactoring_checker import RefactoringChecker

__all__ = [
    "ImplicitBooleanessChecker",
    "NotChecker",
    "RecommendationChecker",
    "RefactoringChecker",
]


def register(linter):
    """Required method to auto register this checker."""
    linter.register_checker(RefactoringChecker(linter))
    linter.register_checker(NotChecker(linter))
    linter.register_checker(RecommendationChecker(linter))
    linter.register_checker(ImplicitBooleanessChecker(linter))
