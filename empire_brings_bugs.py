from typing import List


def unique_tags(payload: dict) -> List[str]:
    result = []
    
    for tag in payload.get('tags', []):
        if tag not in result \
            or type(result[result.index(tag)]) != type(tag):  # "1.0 == True" bug workaround
            result.append(tag)
        
    return result


#############################################

import unittest


class UniqueTagsTestCase(unittest.TestCase):
    def test_request(self):
        tags = [2, "семейное кино", "космос", 1.0, "сага", "эпик", "добро против зла", True, "челмедведосвин", "debug", "ipdb", "PyCharm", "боевик", "эникей", "дарт багус", 5, 6,4, "блокбастер", "кино 2020", 7, 3, 9, 12, "каникулы в космосе", "коварство"]
        payload = {
            "title": "Звездные войны 1: Империя приносит баги",
            "description": "Эпичная сага по поиску багов в старом легаси проекте Империи",
            "tags": [2, "семейное кино", "космос", 1.0, "сага", "эпик", "добро против зла", True, "челмедведосвин", "debug", "ipdb", "PyCharm", "боевик", "боевик", "эникей", "дарт багус", 5, 6,4, "блокбастер", "кино 2020", 7, 3, 9, 12, "каникулы в космосе", "коварство"],
            "version": 17
        }
    
        self.assertCountEqual(tags, unique_tags(payload))
