package app

import traits.Setup
import app.atom.DomainContext
import app.usb.USB
import app.firefly.Firefly

object Alpha extends App with DomainContext with Setup {
  override def main(args: Array[String]) {
    setup(this.OUTPUT_DIR)
    USB.process(sc)
    Firefly.process(sc)
  }
}