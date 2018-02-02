object Hello extends App {

  var list = scala.collection.mutable.LinkedList(1,2,3,4,5,6,7,8,9)

  var currentList = list

  var first = true

  while (currentList!=Nil && currentList.next != Nil) {
    if (first) {
      currentList.elem = currentList.elem * 2
      first = false
    }
    currentList = currentList.next.next
    currentList.elem = currentList.elem * 2
  }

  println(list)
  println(currentList)
}
