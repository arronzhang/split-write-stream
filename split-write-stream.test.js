const SplitWriteStream = require('./split-write-stream.js')
const path = require('path')
const fs = require('fs')
const os = require('os')

const testFile = path.join(os.tmpdir(), 'split-write-stream.data')
const testMetaFile = testFile + '.sws'

beforeEach(() => {
  clean()
})

afterAll(() => {
  clean()
})

function clean() {
  if (fs.existsSync(testFile)) {
    fs.unlinkSync(testFile)
  }
  if (fs.existsSync(testMetaFile)) {
    fs.unlinkSync(testMetaFile)
  }
}

describe('split write stream', () => {
  test('build meta', () => {
    const buildMeta = SplitWriteStream.buildMeta
    expect(buildMeta({ fileSize: 100, ranges: [[0, 99]] })).toEqual({
      fileSize: 100,
      ranges: [[0, 99]]
    })

    expect(buildMeta({ fileSize: 100 })).toEqual({
      fileSize: 100,
      ranges: [
        [0, 19],
        [20, 39],
        [40, 59],
        [60, 79],
        [80, 99]
      ]
    })
    expect(buildMeta({ fileSize: 100, maxPart: 2 })).toEqual({
      fileSize: 100,
      ranges: [
        [0, 49],
        [50, 99]
      ]
    })

    expect(buildMeta({ fileSize: 101, maxPart: 2 })).toEqual({
      fileSize: 101,
      ranges: [
        [0, 50],
        [51, 100]
      ]
    })

    expect(buildMeta({ fileSize: 101, minPartSize: 100, maxPart: 2 })).toEqual({
      fileSize: 101,
      ranges: [
        [0, 99],
        [100, 100]
      ]
    })

    expect(buildMeta({ fileSize: 101, partSize: 50 })).toEqual({
      fileSize: 101,
      ranges: [
        [0, 49],
        [50, 99],
        [100, 100]
      ]
    })
  })

  test('part use fd', done => {
    let stream = new SplitWriteStream(testFile, {
      fileSize: 9,
      maxPart: 3
    })
    stream.on('open', () => {
      let first = stream.partStream(0)
      first.write(Buffer.from('abc'))
      first.end()
      setTimeout(() => {
        stream.partStream(1).write(Buffer.from('abc'))
        stream.partStream(2).write(Buffer.from('abc'))
      }, 100)
    })
    stream.on('finish', () => {
      expect(fs.readFileSync(testFile, 'UTF-8')).toEqual('abcabcabc')
      expect(fs.existsSync(testMetaFile)).toBeFalsy()
      done()
    })
  })

  test('part use path', done => {
    let stream = new SplitWriteStream(testFile, {
      fileSize: 9,
      maxPart: 3
    })

    let first = stream.partStream(0)
    first.write(Buffer.from('abc'))
    first.end()

    setTimeout(() => {
      stream.partStream(1).write(Buffer.from('abc'))
      stream.partStream(2).write(Buffer.from('abc'))
    }, 100)

    stream.on('finish', () => {
      expect(fs.readFileSync(testFile, 'UTF-8')).toEqual('abcabcabc')
      expect(fs.existsSync(testMetaFile)).toBeFalsy()
      done()
    })
  })

  test('reuse', done => {
    let stream = new SplitWriteStream(testFile, {
      fileSize: 9,
      maxPart: 3
    })
    stream.on('open', () => {
      let buf = fs.readFileSync(testFile)
      expect(buf.length).toBe(9)
      let part = stream.partStream(0)
      part.write(Buffer.from('abc'))
      part.end()
      part.on('finish', () => {
        stream.end()
      })
    })

    stream.on('finish', () => {
      let buf = fs.readFileSync(testFile)
      expect(fs.existsSync(testMetaFile)).toBeTruthy()
      let secStream = new SplitWriteStream(testFile, {
        fileSize: 9
      })

      expect(secStream.unfinishParts()[0]).toHaveProperty('index', 1)

      secStream.partStream(1).write('abc')
      secStream.partStream(2).write('abc')
      secStream.on('finish', () => {
        let thirdStream = new SplitWriteStream(testFile, { fileSize: 9 })
        expect(thirdStream.finished).toBeTruthy()
        thirdStream.on('finish', () => {
          done()
        })
      })
    })
  })
})
