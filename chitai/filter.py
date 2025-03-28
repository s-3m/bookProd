def filtering_cover(text: str):
    cover = [
        ("Мягкий переплёт", "Мягкая обложка"),
        ("Мягкий заламинированный картон", "Мягкая обложка"),
        ("ПВХ", "Мягкая обложка"),
        ("Натуральная кожа", "Тканевый переплет"),
        ("Искусственная кожа", "Тканевый переплет"),
        ("Твёрдая ткань", "Тканевый переплет"),
        ("Плотный картон", "Картон"),
        ("Твердый переплёт", "Твердый переплет"),
    ]

    for i in cover:
        if i[0] in text:
            return i[1]
    return text
