package com.bupt.reco.domain

class RecommendedItems {
  private var items: Array[Long] = null

  def getItems: Array[Long] = {
    return items
  }

  def setItems(items: Array[Long]) {
    this.items = items
  }
}
