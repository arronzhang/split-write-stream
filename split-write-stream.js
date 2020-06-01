const path = require('path')
const fs = require('fs')
const assert = require('assert')
const fsp = fs.promises

class PartWriteStream extends fs.WriteStream {
  _write(data, encoding, cb) {
    super._write(data, encoding, err => {
      if (!err) {
        this.emit('writed', data)
      }
      cb(err)
    })
  }

  _writev(data, cb) {
    super._writev(data, err => {
      if (!err) {
        this.emit('writed', data)
      }
      cb(err)
    })
  }
}

class SplitWriteStream extends fs.WriteStream {
  constructor(path, options = {}) {
    assert(path, 'path required for check meta data')
    assert(
      typeof options.fileSize == 'number' && options.fileSize > 0,
      'options.fileSize must bigger than zero for split'
    )

    options.flags = 'r+'

    const fileExists = fs.existsSync(path)
    const metaPath = path + '.' + (options.metaExtname || 'sws')
    const metaExists = fs.existsSync(metaPath)

    if (!fileExists) fs.writeFileSync(path, Buffer.from([]))

    super(path, options)
    this.options = options
    this.metaPath = metaPath

    if (!metaExists && !fileExists) {
      //Fist init meta.
      this._meta = SplitWriteStream.buildMeta(this.options)
      try {
        this._updateMetaFile()
      } catch (er) {
        this._errOrDestory()
      }
    } else if (!metaExists && fileExists) {
      //complate.
      this._meta = {
        fileSize: this.options.fileSize,
        ranges: []
      }
    } else {
      this._meta = this._readMetaFile()
    }

    this._checkEnd()

    this.once('open', () => {
      if (!this.finished && this.fd) {
        //check meta
        try {
          let stat = fs.fstatSync(this.fd)
          if (stat.size === 0) {
            fs.ftruncateSync(this.fd, this._meta.fileSize)
          } else if (stat.size != this._meta.fileSize) {
            //File changed.
            this._errOrDestory(new Error(`File size does not match meta`))
          }
        } catch (er) {
          this._errOrDestory(er)
        }
      }
    })
  }

  _errOrDestory(er) {
    if (er) {
      if (this.autoClose) {
        this.destroy()
      }
      this.emit('error', er)
      return
    }
  }

  static buildMeta({ fileSize, ranges, partSize, minPartSize, maxPart = 5 }) {
    if (!ranges) {
      ranges = []
      let pos = 0
      if (!partSize) {
        partSize = Math.ceil(fileSize / maxPart)
        if (partSize < minPartSize) {
          partSize = minPartSize
        }
      }
      let final = fileSize - 1
      while (pos < fileSize) {
        let end = pos + partSize - 1
        end = Math.min(end, final)
        ranges.push([pos, end])
        pos = end + 1
      }
    }
    return {
      fileSize,
      ranges
    }
  }

  get meta() {
    return this._meta
  }

  _readMetaFile() {
    let data
    try {
      data = JSON.parse(fs.readFileSync(this.metaPath, { encoding: 'UTF-8' }))
    } catch (err) {}
    return data || SplitWriteStream.buildMeta(this.options)
  }

  _updateMetaFile() {
    fs.writeFileSync(this.metaPath, JSON.stringify(this._meta, null, 2))
  }

  _unlinkMetaFile() {
    try {
      if (fs.existsSync(this.metaPath)) fs.unlinkSync(this.metaPath)
    } catch (er) {}
  }

  _checkEnd() {
    let unfinished = this.parts().find(p => !p.finished)
    if (!unfinished) {
      this.finished = true
      this._unlinkMetaFile()
      this.end()
      return true
    }
    return false
  }

  _getRange(index) {
    return this._meta && this._meta.ranges && this._meta.ranges[index]
  }

  unfinishParts() {
    return this.parts().filter(p => !p.finished)
  }

  parts() {
    return this._meta.ranges.map((r, index) => {
      return {
        index,
        finished: r[0] > r[1],
        range: r
      }
    })
  }

  partStream(index) {
    let range = this._getRange(index)
    assert(range, 'Out of range')
    const part = new PartWriteStream(this.path, {
      fd: this.fd,
      start: range[0],
      autoClose: !this.fd,
      flags: 'r+',
      parent: this
    })
    part.on('writed', () => {
      let range = this._getRange(index)
      range[0] = part.start + part.bytesWritten
      try {
        this._updateMetaFile()
      } catch (er) {}
      this._checkEnd()
    })
    return part
  }
}

module.exports = SplitWriteStream
