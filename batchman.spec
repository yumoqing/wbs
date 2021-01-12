# -*- mode: python -*-
a = Analysis(['batchman.py'],
             pathex=['D:\\python\\F\\pywork\\wbs'],
             hiddenimports=[],
             hookspath=None,
             runtime_hooks=None)
pyz = PYZ(a.pure)
exe = EXE(pyz,
          a.scripts,
          a.binaries,
          a.zipfiles,
          a.datas,
          name='wbs.exe',
          debug=False,
          strip=None,
          upx=True,
          console=True )
