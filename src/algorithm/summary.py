# 项目概况相关辅助function
from bs4 import BeautifulSoup


def get_key_loc(info_sequence, pattern):
    """ 获取关键词位置索引
    :param info_sequence: 切分好的文本小段，使用空格、换行符、冒号等将文本分好的list
    :param pattern: 关键词
    :return: 位置（索引） list of int
    """
    if info_sequence is None or not info_sequence:
        return None
    if not isinstance(info_sequence, list):
        raise ValueError('info_sequence is not a list')
    key_locs = []
    for i in range(len(info_sequence)):
        each = info_sequence[i]
        if pattern.findall(each) and len(each) < 800:  # 判断条件预留 长度800
            key_locs.append(i)
    if not key_locs:
        return None
    return key_locs


def _get_one_summary(info_sequence, loc, contain_pattern, remove_pattern):
    if loc is None:
        return None
    # 设定当前索引为关键词所在位置的索引
    index = loc
    # 将index位置的文本置入结果
    res = info_sequence[index]
    res = remove_pattern.sub('', res)
    index += 1
    # 最大索引
    max_index = len(info_sequence) - 1
    # 当索引没有超过最大索引 且 该文本中包含了关键词（例如 桩号 km等关键词的时候）， 将该文本也视为 项目概况
    while (index < max_index and contain_pattern.findall(info_sequence[index])) or not info_sequence[index]:
        res += info_sequence[index]
        index += 1
    return res


def get_summary(info_sequence, key_locs, contain_pattern, remove_pattern):
    if key_locs is None:
        return None
    for loc in key_locs:
        res = _get_one_summary(info_sequence, loc, contain_pattern, remove_pattern)
        if res and len(res) > 20:  # 如果找到了20个字以上的项目规模描述段，那么直接返回结果
            return res
        else:  # 如果没有找到那么，继续查找关键词
            pass
    return None


# 当文本以p标签分割的时候，在标签结束前加入\n
def add_new_line_before_end_ptag(bid_data_obj, p_tag_pattern):
    if not bid_data_obj.is_valid():
        return None
    info = p_tag_pattern.sub(r'\n</p>', bid_data_obj.info)
    info_text = BeautifulSoup(info, 'lxml').text
    return info_text

